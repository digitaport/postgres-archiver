from fastapi import FastAPI, Request, Form, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from psycopg2.extensions import adapt, register_adapter
import logging
from typing import List, Dict, Any, Union
import json
import decimal
import datetime
import uuid
import re
import connections
from fastapi import BackgroundTasks

app = FastAPI(title="PostgreSQL Database Archiver")
templates = Jinja2Templates(directory="templates")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_safe_identifier(name: str) -> bool:
    return bool(name) and bool(IDENTIFIER_RE.match(name))


def _quote_identifier(name: str) -> str:
    if not _is_safe_identifier(name):
        raise ValueError(f"Invalid SQL identifier: {name}")
    return f'"{name}"'


def _normalize_index_name(name: str, max_len: int = 63) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if len(cleaned) <= max_len:
        return cleaned
    return cleaned[:max_len]

# Global storage for migration jobs
migration_jobs = {}

class MigrationJob:
    def __init__(self, job_id: str, source_table: str, target_table: str, total_rows: int):
        self.id = job_id
        self.status = "running"  # running, completed, failed
        self.source_table = source_table
        self.target_table = target_table
        self.total_rows = total_rows
        self.processed_rows = 0
        self.message = "Starting migration..."
        self.error = None
        self.result_data = {}


class DatabaseConnection:
    def __init__(self, host: str, port: int, database: str, username: str, password: str):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.connection = None
    
    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password
            )
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
    
    def disconnect(self):
        if self.connection:
            self.connection.close()
    
    def get_tables(self) -> List[str]:
        """Get list of all tables in the database"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get column information for a specific table"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT column_name, data_type, is_nullable, character_maximum_length, 
                           numeric_precision, numeric_scale, column_default
                    FROM information_schema.columns 
                    WHERE table_name = %s 
                    AND table_schema = 'public'
                    ORDER BY ordinal_position
                """, (table_name,))
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            return []
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    )
                """, (table_name,))
                return cursor.fetchone()[0]
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False
    
    def create_table_from_schema(self, table_name: str, columns: List[Dict[str, Any]]) -> bool:
        """Create a new table with the specified schema"""
        try:
            # Build column definitions
            col_definitions = []
            for col in columns:
                col_def = f"{col['column_name']} {col['data_type']}"
                
                # Add length for character types
                if col['character_maximum_length'] and col['data_type'] in ['character varying', 'character', 'varchar', 'char']:
                    col_def += f"({col['character_maximum_length']})"
                
                # Add precision/scale for numeric types
                elif col['numeric_precision'] and col['data_type'] in ['numeric', 'decimal']:
                    if col['numeric_scale']:
                        col_def += f"({col['numeric_precision']},{col['numeric_scale']})"
                    else:
                        col_def += f"({col['numeric_precision']})"
                
                # Add NOT NULL constraint
                if col['is_nullable'] == 'NO':
                    col_def += " NOT NULL"
                
                # Add default value (excluding sequence defaults for serial columns)
                if col['column_default'] and not col['column_default'].startswith('nextval'):
                    col_def += f" DEFAULT {col['column_default']}"
                
                col_definitions.append(col_def)
            
            # Create the table
            create_query = f"""
            CREATE TABLE {table_name} (
                {', '.join(col_definitions)}
            )
            """
            
            with self.connection.cursor() as cursor:
                cursor.execute(create_query)
                self.connection.commit()
                return True
                
        except Exception as e:
            logger.error(f"Error creating table: {e}")
            self.connection.rollback()
            return False
    
    def validate_schema_compatibility(self, source_columns: List[Dict[str, Any]], 
                                     target_columns: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate that target table schema is compatible with source"""
        result = {
            'compatible': True,
            'issues': [],
            'warnings': []
        }
        
        # Create dictionaries for easier comparison
        source_cols = {col['column_name']: col for col in source_columns}
        target_cols = {col['column_name']: col for col in target_columns}
        
        # Check for missing columns in target
        missing_cols = set(source_cols.keys()) - set(target_cols.keys())
        if missing_cols:
            result['compatible'] = False
            result['issues'].append(f"Missing columns in target: {', '.join(missing_cols)}")
        
        # Check for extra columns in target (warning only)
        extra_cols = set(target_cols.keys()) - set(source_cols.keys())
        if extra_cols:
            result['warnings'].append(f"Extra columns in target: {', '.join(extra_cols)}")
        
        # Check data type compatibility for common columns
        for col_name in source_cols.keys() & target_cols.keys():
            source_col = source_cols[col_name]
            target_col = target_cols[col_name]
            
            # Check data type compatibility
            if not self._are_types_compatible(source_col['data_type'], target_col['data_type']):
                result['compatible'] = False
                result['issues'].append(
                    f"Incompatible data type for column '{col_name}': "
                    f"source={source_col['data_type']}, target={target_col['data_type']}"
                )
            
            # Check length constraints for character types
            if (source_col['data_type'] in ['character varying', 'varchar', 'character', 'char'] and
                target_col['data_type'] in ['character varying', 'varchar', 'character', 'char']):
                
                source_len = source_col.get('character_maximum_length')
                target_len = target_col.get('character_maximum_length')
                
                if source_len and target_len and source_len > target_len:
                    result['compatible'] = False
                    result['issues'].append(
                        f"Column '{col_name}' max length in source ({source_len}) "
                        f"exceeds target ({target_len})"
                    )
        
        return result
    
    def _are_types_compatible(self, source_type: str, target_type: str) -> bool:
        """Check if two PostgreSQL data types are compatible for data insertion"""
        # Normalize type names
        type_aliases = {
            'int4': 'integer',
            'int8': 'bigint',
            'int2': 'smallint',
            'varchar': 'character varying',
            'char': 'character',
            'bool': 'boolean',
            'float8': 'double precision',
            'float4': 'real',
            'timestamptz': 'timestamp with time zone'
        }
        
        source_normalized = type_aliases.get(source_type, source_type)
        target_normalized = type_aliases.get(target_type, target_type)
        
        # Exact match
        if source_normalized == target_normalized:
            return True
        
        # Compatible type groups
        compatible_groups = [
            {'integer', 'bigint', 'smallint', 'numeric', 'decimal'},
            {'character varying', 'character', 'text'},
            {'timestamp', 'timestamp with time zone', 'timestamp without time zone'},
            {'real', 'double precision', 'numeric', 'decimal'}
        ]
        
        for group in compatible_groups:
            if source_normalized in group and target_normalized in group:
                return True
        
        return False
    
    def _prepare_value_for_insert(self, value):
        """Prepare a value for database insertion, handling complex data types"""
        if value is None:
            return None
        elif isinstance(value, (dict, list)):
            # For JSON/JSONB columns, use psycopg2's Json adapter
            return Json(value)
        elif isinstance(value, decimal.Decimal):
            # Keep decimal as is
            return value
        elif isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            # Keep datetime objects as is
            return value
        elif isinstance(value, uuid.UUID):
            # Convert UUID to string
            return str(value)
        elif isinstance(value, bytes):
            # Keep bytes as is for bytea columns
            return value
        elif isinstance(value, memoryview):
            # Convert memoryview to bytes
            return bytes(value)
        elif hasattr(value, '__dict__') and not isinstance(value, (str, int, float, bool)):
            # Handle other complex objects
            try:
                # Try to serialize as JSON first
                return Json(value)
            except (TypeError, ValueError):
                # Fall back to string representation
                return str(value)
        else:
            # Return primitive types as is
            return value
    
    def execute_query(self, query: str, params=None):
        """Execute a query and return results"""
        try:
            with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                    
                if query.strip().upper().startswith('SELECT'):
                    return cursor.fetchall()
                else:
                    self.connection.commit()
                    return cursor.rowcount
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            logger.error(f"Query: {query}")
            if params:
                logger.error(f"Parameters: {params}")
            self.connection.rollback()
            raise e

    def has_covering_index(self, table_name: str, columns: List[str]) -> bool:
        """Check if an index exists whose leading columns match the provided columns."""
        if not columns:
            return False

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT array_agg(att.attname ORDER BY key_pos.n) AS idx_columns
                    FROM pg_index idx
                    JOIN pg_class tbl ON tbl.oid = idx.indrelid
                    JOIN pg_namespace ns ON ns.oid = tbl.relnamespace
                    JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS key_pos(attnum, n)
                        ON key_pos.attnum > 0
                    JOIN pg_attribute att ON att.attrelid = tbl.oid AND att.attnum = key_pos.attnum
                    WHERE ns.nspname = 'public'
                      AND tbl.relname = %s
                    GROUP BY idx.indexrelid
                    """,
                    (table_name,)
                )

                wanted = [c.lower() for c in columns]
                for row in cursor.fetchall():
                    idx_cols = [c.lower() for c in (row[0] or [])]
                    if len(idx_cols) >= len(wanted) and idx_cols[:len(wanted)] == wanted:
                        return True
                return False
        except Exception as e:
            logger.warning(f"Error checking indexes for table {table_name}: {e}")
            return False

    def explain_uses_seq_scan(self, table_name: str, where_clause: str) -> bool:
        """Return True if plan shows a sequential scan on the given table."""
        try:
            table_sql = _quote_identifier(table_name)
            explain_query = f"EXPLAIN (FORMAT JSON) SELECT 1 FROM {table_sql} WHERE {where_clause}"

            with self.connection.cursor() as cursor:
                cursor.execute(explain_query)
                raw = cursor.fetchone()[0]

            if isinstance(raw, str):
                plan_data = json.loads(raw)
            else:
                plan_data = raw

            root = plan_data[0].get("Plan", {}) if isinstance(plan_data, list) and plan_data else {}

            def _walk(node: Dict[str, Any]) -> bool:
                if node.get("Node Type") == "Seq Scan" and node.get("Relation Name") == table_name:
                    return True
                for child in node.get("Plans", []) or []:
                    if _walk(child):
                        return True
                return False

            return _walk(root)
        except Exception as e:
            logger.warning(f"Could not analyze query plan for {table_name}: {e}")
            return False

    def execute_ddl_autocommit(self, query: str):
        """Execute DDL that may require autocommit (e.g. CREATE INDEX CONCURRENTLY)."""
        if not self.connection:
            raise RuntimeError("Database connection is not established")

        original_autocommit = self.connection.autocommit
        try:
            # SELECTs on this shared connection leave psycopg2 inside a transaction.
            # CREATE INDEX CONCURRENTLY requires running outside any transaction block.
            self.connection.rollback()
            self.connection.autocommit = True
            with self.connection.cursor() as cursor:
                cursor.execute(query)
        finally:
            self.connection.autocommit = original_autocommit


def recommend_source_index(db: DatabaseConnection, table_name: str, column_names: List[str], where_clause: str) -> Dict[str, Any]:
    """Return index recommendation metadata based on index coverage + EXPLAIN plan."""
    recommendation = {
        "recommended": False,
        "reason": "",
        "index_sql": "",
        "columns": []
    }

    if not table_name or not column_names:
        recommendation["reason"] = "No filter columns were provided."
        return recommendation

    if not _is_safe_identifier(table_name) or any(not _is_safe_identifier(c) for c in column_names):
        recommendation["reason"] = "Could not analyze index recommendation due to invalid identifier format."
        return recommendation

    # If a covering index already exists, no recommendation needed.
    if db.has_covering_index(table_name, column_names):
        recommendation["reason"] = "A covering index already exists for the selected filter columns."
        return recommendation

    # Recommend only when planner is currently using sequential scan for this filter.
    if not db.explain_uses_seq_scan(table_name, where_clause):
        recommendation["reason"] = "Planner is not using a sequential scan for this query."
        return recommendation

    table_sql = _quote_identifier(table_name)
    columns_sql = ", ".join(_quote_identifier(c) for c in column_names)
    idx_name = _normalize_index_name(f"idx_{table_name}_{'_'.join(column_names)}_archiver")

    recommendation["recommended"] = True
    recommendation["reason"] = "Query plan shows sequential scan and no covering index for selected columns."
    recommendation["index_sql"] = f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} ON {table_sql} ({columns_sql});"
    recommendation["columns"] = column_names
    return recommendation


def build_preview_context(db: DatabaseConnection, source_table_name: str, target_table_name: str,
                         column_names: List[str], conditions: List[str]) -> Dict[str, Any]:
    """Build shared preview context used by preview and index-creation endpoints."""
    where_clause = build_where_clause(column_names, conditions)

    preview_query = f"SELECT * FROM {source_table_name} WHERE {where_clause} LIMIT 10"
    preview_rows = db.execute_query(preview_query)

    count_query = f"SELECT COUNT(*) as count FROM {source_table_name} WHERE {where_clause}"
    count_result = db.execute_query(count_query)
    total_count = count_result[0]['count'] if count_result else 0

    index_recommendation = recommend_source_index(
        db,
        source_table_name,
        column_names,
        where_clause
    )

    return {
        "source_table_name": source_table_name,
        "target_table_name": target_table_name,
        "column_names": column_names,
        "conditions": conditions,
        "where_clause": where_clause,
        "preview_data": preview_rows,
        "total_count": total_count,
        "columns": list(preview_rows[0].keys()) if preview_rows else [],
        "index_recommendation": index_recommendation
    }

def build_where_clause(column_names: List[str], conditions: List[str]) -> str:
    """Build a WHERE clause from multiple column/condition pairs combined with AND"""
    if not column_names or not conditions:
        raise ValueError("Column names and conditions cannot be empty")
    
    if len(column_names) != len(conditions):
        raise ValueError("Number of column names must match number of conditions")
    
    where_parts = []
    for col, cond in zip(column_names, conditions):
        if cond is None:
            raise ValueError(f"Condition cannot be empty for column '{col}'")

        cond_text = str(cond).strip()
        if not cond_text:
            raise ValueError(f"Condition cannot be empty for column '{col}'")

        # Support users entering either:
        # 1) only the predicate part (e.g. ">= NOW() - INTERVAL '7 days'")
        # 2) the full expression (e.g. "created >= NOW() - INTERVAL '7 days'")
        # Avoid duplicating the column in case (2).
        col_pattern = re.escape(col)
        includes_column_already = bool(
            re.match(rf'^(?:"{col_pattern}"|{col_pattern})\s+', cond_text, flags=re.IGNORECASE)
        )

        if includes_column_already:
            where_parts.append(cond_text)
        else:
            where_parts.append(f"{col} {cond_text}")
    
    return " AND ".join(where_parts)

# Global variables to store database connections
source_db = None
target_db = None

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# API endpoints for connection management
@app.get("/api/connections")
async def get_connections():
    """Get all saved connections (without passwords)"""
    return {"connections": connections.get_connections_without_passwords()}

@app.get("/api/connections/{connection_id}")
async def get_connection(connection_id: str):
    """Get a specific connection with full details including password"""
    conn = connections.get_connection(connection_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    return conn

@app.post("/api/connections")
async def create_connection(
    name: str = Form(...),
    source_host: str = Form(...),
    source_port: int = Form(...),
    source_database: str = Form(...),
    source_username: str = Form(...),
    source_password: str = Form(default=""),
    target_host: str = Form(...),
    target_port: int = Form(...),
    target_database: str = Form(...),
    target_username: str = Form(...),
    target_password: str = Form(default="")
):
    """Create a new saved connection"""
    source = {
        "host": source_host,
        "port": source_port,
        "database": source_database,
        "username": source_username,
        "password": source_password
    }
    
    target = {
        "host": target_host,
        "port": target_port,
        "database": target_database,
        "username": target_username,
        "password": target_password
    }
    
    new_conn = connections.add_connection(name, source, target)
    return new_conn

@app.post("/api/connections/{connection_id}/invert")
async def invert_connection(connection_id: str):
    """Create a new saved connection with source and target swapped"""
    inverted_conn = connections.invert_connection(connection_id)
    if not inverted_conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    return inverted_conn

@app.put("/api/connections/{connection_id}")
async def update_connection(
    connection_id: str,
    name: str = Form(...),
    source_host: str = Form(...),
    source_port: int = Form(...),
    source_database: str = Form(...),
    source_username: str = Form(...),
    source_password: str = Form(default=""),
    target_host: str = Form(...),
    target_port: int = Form(...),
    target_database: str = Form(...),
    target_username: str = Form(...),
    target_password: str = Form(default="")
):
    """Update an existing connection"""
    source = {
        "host": source_host,
        "port": source_port,
        "database": source_database,
        "username": source_username,
        "password": source_password
    }
    
    target = {
        "host": target_host,
        "port": target_port,
        "database": target_database,
        "username": target_username,
        "password": target_password
    }
    
    updated_conn = connections.update_connection(connection_id, name, source, target)
    if not updated_conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    return updated_conn

@app.delete("/api/connections/{connection_id}")
async def delete_connection(connection_id: str):
    """Delete a saved connection"""
    success = connections.delete_connection(connection_id)
    if not success:
        raise HTTPException(status_code=404, detail="Connection not found")
    return {"success": True, "message": "Connection deleted"}

@app.post("/connect_saved")
async def connect_saved(
    request: Request,
    connection_id: str = Form(...)
):
    """Connect using a saved connection"""
    global source_db, target_db
    
    conn = connections.get_connection(connection_id)
    if not conn:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    source = conn.get("source", {})
    target = conn.get("target", {})
    
    # Test source database connection
    source_db = DatabaseConnection(
        source.get("host"),
        source.get("port"),
        source.get("database"),
        source.get("username"),
        source.get("password", "")
    )
    if not source_db.connect():
        raise HTTPException(status_code=400, detail="Failed to connect to source database")
    
    # Test target database connection
    target_db = DatabaseConnection(
        target.get("host"),
        target.get("port"),
        target.get("database"),
        target.get("username"),
        target.get("password", "")
    )
    if not target_db.connect():
        source_db.disconnect()
        raise HTTPException(status_code=400, detail="Failed to connect to target database")
    
    # Get tables from source database
    tables = source_db.get_tables()
    
    return templates.TemplateResponse("tables.html", {
        "request": request,
        "tables": tables,
        "source_db_info": f"{source.get('database')}@{source.get('host')}",
        "target_db_info": f"{target.get('database')}@{target.get('host')}"
    })


@app.post("/connect")
async def connect_databases(
    request: Request,
    source_host: str = Form(...),
    source_port: int = Form(...),
    source_database: str = Form(...),
    source_username: str = Form(...),
    source_password: str = Form(default=""),
    target_host: str = Form(...),
    target_port: int = Form(...),
    target_database: str = Form(...),
    target_username: str = Form(...),
    target_password: str = Form(default="")
):
    global source_db, target_db
    
    # Test source database connection
    source_db = DatabaseConnection(source_host, source_port, source_database, source_username, source_password)
    if not source_db.connect():
        raise HTTPException(status_code=400, detail="Failed to connect to source database")
    
    # Test target database connection
    target_db = DatabaseConnection(target_host, target_port, target_database, target_username, target_password)
    if not target_db.connect():
        source_db.disconnect()
        raise HTTPException(status_code=400, detail="Failed to connect to target database")
    
    # Get tables from source database
    tables = source_db.get_tables()
    
    return templates.TemplateResponse("tables.html", {
        "request": request,
        "tables": tables,
        "source_db_info": f"{source_database}@{source_host}",
        "target_db_info": f"{target_database}@{target_host}"
    })

@app.post("/select_table")
async def select_table(
    request: Request,
    table_name: str = Form(...)
):
    global source_db, target_db
    
    if not source_db or not target_db:
        raise HTTPException(status_code=400, detail="No database connections")
    
    # Get columns for the selected source table
    source_columns = source_db.get_table_columns(table_name)
    
    # Get available tables in target database
    target_tables = target_db.get_tables()
    
    return templates.TemplateResponse("target_table.html", {
        "request": request,
        "source_table_name": table_name,
        "source_columns": source_columns,
        "target_tables": target_tables
    })

@app.post("/select_target_table")
async def select_target_table(
    request: Request,
    source_table_name: str = Form(...),
    target_table_option: str = Form(...),  # "existing" or "new"
    existing_table_name: str = Form(default=""),
    new_table_name: str = Form(default="")
):
    global source_db, target_db
    
    if not source_db or not target_db:
        raise HTTPException(status_code=400, detail="No database connections")
    
    # Get source table schema
    source_columns = source_db.get_table_columns(source_table_name)
    
    target_table_name = ""
    schema_validation = None
    target_exists = False
    
    if target_table_option == "existing":
        target_table_name = existing_table_name
        target_exists = target_db.table_exists(target_table_name)
        
        if not target_exists:
            raise HTTPException(status_code=400, detail=f"Target table '{target_table_name}' does not exist")
        
        # Validate schema compatibility
        target_columns = target_db.get_table_columns(target_table_name)
        schema_validation = target_db.validate_schema_compatibility(source_columns, target_columns)
        
        if not schema_validation['compatible']:
            return templates.TemplateResponse("schema_validation.html", {
                "request": request,
                "source_table_name": source_table_name,
                "target_table_name": target_table_name,
                "validation": schema_validation,
                "source_columns": source_columns,
                "target_columns": target_columns
            })
    
    elif target_table_option == "new":
        target_table_name = new_table_name
        
        if not target_table_name:
            raise HTTPException(status_code=400, detail="New table name is required")
        
        # Check if table already exists
        if target_db.table_exists(target_table_name):
            raise HTTPException(status_code=400, detail=f"Table '{target_table_name}' already exists in target database")
        
        # Create the new table
        if not target_db.create_table_from_schema(target_table_name, source_columns):
            raise HTTPException(status_code=500, detail="Failed to create target table")
        
        target_exists = True
        schema_validation = {'compatible': True, 'issues': [], 'warnings': []}
    
    return templates.TemplateResponse("columns.html", {
        "request": request,
        "source_table_name": source_table_name,
        "target_table_name": target_table_name,
        "columns": source_columns,
        "schema_validation": schema_validation
    })

@app.post("/preview_data")
async def preview_data(
    request: Request,
    source_table_name: str = Form(...),
    target_table_name: str = Form(...),
    column_name: Union[str, List[str]] = Form(None),
    condition: Union[str, List[str]] = Form(None)
):
    global source_db
    
    if not source_db:
        raise HTTPException(status_code=400, detail="No source database connection")
    
    try:
        # Handle both single condition (backward compatibility) and multiple conditions
        if isinstance(column_name, str):
            column_names = [column_name]
            conditions = [condition]
        else:
            column_names = column_name if column_name else []
            conditions = condition if condition else []

        context = build_preview_context(source_db, source_table_name, target_table_name, column_names, conditions)
        context["request"] = request
        context["index_create_message"] = ""
        context["index_create_error"] = ""

        return templates.TemplateResponse("preview.html", context)
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error previewing data: {str(e)}")


@app.post("/create_recommended_index")
async def create_recommended_index(
    request: Request,
    source_table_name: str = Form(...),
    target_table_name: str = Form(...),
    column_name: Union[str, List[str]] = Form(None),
    condition: Union[str, List[str]] = Form(None)
):
    """Create recommended source index from preview page and return updated preview."""
    global source_db

    if not source_db:
        raise HTTPException(status_code=400, detail="No source database connection")

    try:
        if isinstance(column_name, str):
            column_names = [column_name]
            conditions = [condition]
        else:
            column_names = column_name if column_name else []
            conditions = condition if condition else []

        where_clause = build_where_clause(column_names, conditions)
        recommendation = recommend_source_index(source_db, source_table_name, column_names, where_clause)

        index_create_message = ""
        index_create_error = ""

        if recommendation.get("recommended") and recommendation.get("index_sql"):
            try:
                source_db.execute_ddl_autocommit(recommendation["index_sql"])
                index_create_message = "Recommended index created successfully on source database."
            except Exception as e:
                logger.error(f"Error creating recommended index: {e}")
                index_create_error = f"Failed to create index: {str(e)}"
        else:
            index_create_message = "No index creation needed for the current query."

        context = build_preview_context(source_db, source_table_name, target_table_name, column_names, conditions)
        context["request"] = request
        context["index_create_message"] = index_create_message
        context["index_create_error"] = index_create_error
        return templates.TemplateResponse("preview.html", context)

    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error creating index: {str(e)}")

def run_migration_background(job_id: str, source_params: dict, target_params: dict, 
                            source_table: str, target_table: str, where_clause: str,
                            column_names: List[str], conditions: List[str]):
    job = migration_jobs.get(job_id)
    if not job:
        return

    source_db_conn = DatabaseConnection(**source_params)
    target_db_conn = DatabaseConnection(**target_params)
    
    try:
        if not source_db_conn.connect():
            raise Exception("Failed to connect to source database")
        if not target_db_conn.connect():
            raise Exception("Failed to connect to target database")
            
        # Get columns to ensure consistent ordering
        source_cols_info = source_db_conn.get_table_columns(source_table)
        columns = [col['column_name'] for col in source_cols_info]
        inserted_rows = 0
        
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        # ON CONFLICT DO NOTHING makes the migration idempotent so reruns skip
        # rows that were already copied in previous partial runs.
        insert_query = f"INSERT INTO {target_table} ({columns_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
        
        # Use server-side cursor for reading
        with source_db_conn.connection.cursor(name=f"mig_{job_id}") as read_cursor:
            read_cursor.execute(f"SELECT {columns_str} FROM {source_table} WHERE {where_clause}")
            
            batch_size = 100
            
            while True:
                rows = read_cursor.fetchmany(batch_size)
                if not rows:
                    break
                
                # Process batch
                for row in rows:
                    # row is a tuple
                    values = []
                    for i, value in enumerate(row):
                        prepared_value = target_db_conn._prepare_value_for_insert(value)
                        values.append(prepared_value)
                    
                    inserted_rows += target_db_conn.execute_query(insert_query, values)
                
                job.processed_rows += len(rows)
                job.message = f"Processed {job.processed_rows} of {job.total_rows} rows..."
        
        job.status = "completed"
        job.message = "Migration completed successfully"
        job.result_data = {
            "success": True,
            "message": (
                f"Migration finished: scanned {job.processed_rows} rows, "
                f"inserted {inserted_rows} new rows, "
                f"skipped {job.processed_rows - inserted_rows} existing rows in target table '{target_table}'"
            ),
            "archived_count": inserted_rows,
            "scanned_count": job.processed_rows,
            "skipped_existing_count": job.processed_rows - inserted_rows,
            "source_table_name": source_table,
            "target_table_name": target_table,
            "column_names": column_names,
            "conditions": conditions,
            "where_clause": where_clause
        }
        
    except Exception as e:
        job.status = "failed"
        job.error = str(e)
        job.message = f"Error: {str(e)}"
        logger.error(f"Migration job {job_id} failed: {e}")
    finally:
        source_db_conn.disconnect()
        target_db_conn.disconnect()

@app.get("/api/migration_status/{job_id}")
async def get_migration_status(job_id: str):
    job = migration_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    return {
        "id": job.id,
        "status": job.status,
        "processed_rows": job.processed_rows,
        "total_rows": job.total_rows,
        "message": job.message,
        "error": job.error,
        "result_data": job.result_data
    }

@app.get("/migration_progress/{job_id}", response_class=HTMLResponse)
async def migration_progress(request: Request, job_id: str):
    job = migration_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
        
    return templates.TemplateResponse("progress.html", {
        "request": request,
        "job": job
    })

@app.post("/archive_data")
async def archive_data(
    request: Request,
    background_tasks: BackgroundTasks,
    source_table_name: str = Form(...),
    target_table_name: str = Form(...),
    column_name: Union[str, List[str]] = Form(None),
    condition: Union[str, List[str]] = Form(None)
):
    global source_db, target_db
    
    if not source_db or not target_db:
        raise HTTPException(status_code=400, detail="Database connections not established")
    
    try:
        # Handle both single condition (backward compatibility) and multiple conditions
        if isinstance(column_name, str):
            column_names = [column_name]
            conditions = [condition]
        else:
            column_names = column_name if column_name else []
            conditions = condition if condition else []
        
        # Build WHERE clause
        where_clause = build_where_clause(column_names, conditions)
        
        # Count total rows to migrate
        count_query = f"SELECT COUNT(*) as count FROM {source_table_name} WHERE {where_clause}"
        count_result = source_db.execute_query(count_query)
        total_count = count_result[0]['count'] if count_result else 0
        
        if total_count == 0:
            return templates.TemplateResponse("result.html", {
                "request": request,
                "success": False,
                "message": "No data found matching the condition",
                "archived_count": 0
            })
            
        # Create migration job
        job_id = str(uuid.uuid4())
        job = MigrationJob(job_id, source_table_name, target_table_name, total_count)
        migration_jobs[job_id] = job
        
        # Prepare connection parameters for the background task
        # We need to pass the connection details, not the connection objects themselves
        # since they are not thread-safe and we want independent connections
        source_params = {
            "host": source_db.host,
            "port": source_db.port,
            "database": source_db.database,
            "username": source_db.username,
            "password": source_db.password
        }
        
        target_params = {
            "host": target_db.host,
            "port": target_db.port,
            "database": target_db.database,
            "username": target_db.username,
            "password": target_db.password
        }
        
        # Start background task
        background_tasks.add_task(
            run_migration_background,
            job_id,
            source_params,
            target_params,
            source_table_name,
            target_table_name,
            where_clause,
            column_names,
            conditions
        )
        
        # Redirect to progress page
        # We can't use RedirectResponse because we want to render a template initially
        # or we can just render the template which will then poll
        return templates.TemplateResponse("progress.html", {
            "request": request,
            "job": job
        })
    
    except Exception as e:
        logger.error(f"Error starting archive job: {e}")
        return templates.TemplateResponse("result.html", {
            "request": request,
            "success": False,
            "message": f"Error starting archive job: {str(e)}",
            "archived_count": 0
        })

@app.post("/execute_delete")
async def execute_delete(
    request: Request,
    source_table_name: str = Form(...),
    target_table_name: str = Form(...),
    column_name: Union[str, List[str]] = Form(None),
    condition: Union[str, List[str]] = Form(None),
    archived_count: int = Form(...)
):
    """Show confirmation page before executing delete"""
    global source_db
    
    if not source_db:
        raise HTTPException(status_code=400, detail="No source database connection")
    
    try:
        # Handle both single condition (backward compatibility) and multiple conditions
        if isinstance(column_name, str):
            column_names = [column_name]
            conditions = [condition]
        else:
            column_names = column_name if column_name else []
            conditions = condition if condition else []
        
        # Build WHERE clause
        where_clause = build_where_clause(column_names, conditions)
        
        # Get a preview of rows that will be deleted (first 5 rows)
        preview_query = f"SELECT * FROM {source_table_name} WHERE {where_clause} LIMIT 5"
        preview_rows = source_db.execute_query(preview_query)
        
        # Get current count to verify it matches the archived count
        count_query = f"SELECT COUNT(*) as count FROM {source_table_name} WHERE {where_clause}"
        count_result = source_db.execute_query(count_query)
        current_count = count_result[0]['count'] if count_result else 0
        
        delete_query = f"DELETE FROM {source_table_name} WHERE {where_clause}"
        
        return templates.TemplateResponse("confirm_delete.html", {
            "request": request,
            "source_table_name": source_table_name,
            "target_table_name": target_table_name,
            "column_names": column_names,
            "conditions": conditions,
            "where_clause": where_clause,
            "archived_count": archived_count,
            "current_count": current_count,
            "row_count": current_count,
            "delete_query": delete_query,
            "preview_rows": preview_rows,
            "columns": list(preview_rows[0].keys()) if preview_rows else []
        })
        
    except Exception as e:
        logger.error(f"Error preparing delete confirmation: {e}")
        raise HTTPException(status_code=400, detail=f"Error preparing delete: {str(e)}")

@app.post("/execute_delete_confirmed")
async def execute_delete_confirmed(
    request: Request,
    source_table_name: str = Form(...),
    target_table_name: str = Form(...),
    column_name: Union[str, List[str]] = Form(None),
    condition: Union[str, List[str]] = Form(None),
    archived_count: int = Form(...)
):
    """Actually execute the DELETE query after confirmation"""
    global source_db
    
    if not source_db:
        raise HTTPException(status_code=400, detail="No source database connection")
    
    try:
        # Handle both single condition (backward compatibility) and multiple conditions
        if isinstance(column_name, str):
            column_names = [column_name]
            conditions = [condition]
        else:
            column_names = column_name if column_name else []
            conditions = condition if condition else []
        
        # Build WHERE clause
        where_clause = build_where_clause(column_names, conditions)
        
        # Execute the DELETE query
        delete_query = f"DELETE FROM {source_table_name} WHERE {where_clause}"
        deleted_count = source_db.execute_query(delete_query)
        
        return templates.TemplateResponse("delete_result.html", {
            "request": request,
            "success": True,
            "message": f"Successfully deleted {deleted_count} rows from source table",
            "deleted_count": deleted_count,
            "archived_count": archived_count,
            "source_table_name": source_table_name,
            "target_table_name": target_table_name,
            "column_names": column_names,
            "conditions": conditions,
            "where_clause": where_clause,
            "delete_query": delete_query
        })
    
    except Exception as e:
        logger.error(f"Error executing delete: {e}")
        return templates.TemplateResponse("delete_result.html", {
            "request": request,
            "success": False,
            "message": f"Error deleting data: {str(e)}",
            "deleted_count": 0,
            "archived_count": archived_count,
            "source_table_name": source_table_name,
            "target_table_name": target_table_name,
            "column_names": column_names if 'column_names' in locals() else [],
            "conditions": conditions if 'conditions' in locals() else [],
            "where_clause": where_clause if 'where_clause' in locals() else "",
            "delete_query": f"DELETE FROM {source_table_name} WHERE {where_clause}" if 'where_clause' in locals() else ""
        })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
