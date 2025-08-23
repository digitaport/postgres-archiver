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

app = FastAPI(title="PostgreSQL Database Archiver")
templates = Jinja2Templates(directory="templates")

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

# Global variables to store database connections
source_db = None
target_db = None

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/connect")
async def connect_databases(
    request: Request,
    source_host: str = Form(...),
    source_port: int = Form(...),
    source_database: str = Form(...),
    source_username: str = Form(...),
    source_password: str = Form(...),
    target_host: str = Form(...),
    target_port: int = Form(...),
    target_database: str = Form(...),
    target_username: str = Form(...),
    target_password: str = Form(...)
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
    column_name: str = Form(...),
    condition: str = Form(...)
):
    global source_db
    
    if not source_db:
        raise HTTPException(status_code=400, detail="No source database connection")
    
    try:
        # Preview query to show matching rows
        preview_query = f"SELECT * FROM {source_table_name} WHERE {column_name} {condition} LIMIT 10"
        preview_data = source_db.execute_query(preview_query)
        
        # Count total matching rows
        count_query = f"SELECT COUNT(*) as count FROM {source_table_name} WHERE {column_name} {condition}"
        count_result = source_db.execute_query(count_query)
        total_count = count_result[0]['count'] if count_result else 0
        
        return templates.TemplateResponse("preview.html", {
            "request": request,
            "source_table_name": source_table_name,
            "target_table_name": target_table_name,
            "column_name": column_name,
            "condition": condition,
            "preview_data": preview_data,
            "total_count": total_count,
            "columns": list(preview_data[0].keys()) if preview_data else []
        })
    
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error previewing data: {str(e)}")

@app.post("/archive_data")
async def archive_data(
    request: Request,
    source_table_name: str = Form(...),
    target_table_name: str = Form(...),
    column_name: str = Form(...),
    condition: str = Form(...)
):
    global source_db, target_db
    
    if not source_db or not target_db:
        raise HTTPException(status_code=400, detail="Database connections not established")
    
    try:
        # Get the data to be archived
        select_query = f"SELECT * FROM {source_table_name} WHERE {column_name} {condition}"
        data_to_archive = source_db.execute_query(select_query)
        
        if not data_to_archive:
            return templates.TemplateResponse("result.html", {
                "request": request,
                "success": False,
                "message": "No data found matching the condition",
                "archived_count": 0
            })
        
        # Get column names for insert query
        columns = list(data_to_archive[0].keys())
        columns_str = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        
        # Insert data into target database
        insert_query = f"INSERT INTO {target_table_name} ({columns_str}) VALUES ({placeholders})"
        
        archived_count = 0
        for row in data_to_archive:
            try:
                values = []
                for col in columns:
                    value = row[col]
                    # Handle complex data types that psycopg2 can't adapt automatically
                    prepared_value = target_db._prepare_value_for_insert(value)
                    values.append(prepared_value)
                    logger.debug(f"Column {col}: {type(value).__name__} -> {type(prepared_value).__name__}")
                
                target_db.execute_query(insert_query, values)
                archived_count += 1
                
            except Exception as row_error:
                logger.error(f"Error inserting row {archived_count + 1}: {row_error}")
                logger.error(f"Row data types: {[(col, type(row[col]).__name__, row[col]) for col in columns]}")
                logger.error(f"Prepared values types: {[(i, type(v).__name__, v) for i, v in enumerate(values)]}")
                raise row_error
        
        return templates.TemplateResponse("result.html", {
            "request": request,
            "success": True,
            "message": f"Successfully archived {archived_count} rows to target table '{target_table_name}'",
            "archived_count": archived_count,
            "source_table_name": source_table_name,
            "target_table_name": target_table_name,
            "column_name": column_name,
            "condition": condition,
            "delete_query": f"DELETE FROM {source_table_name} WHERE {column_name} {condition}"
        })
    
    except Exception as e:
        logger.error(f"Error archiving data: {e}")
        return templates.TemplateResponse("result.html", {
            "request": request,
            "success": False,
            "message": f"Error archiving data: {str(e)}",
            "archived_count": 0
        })

@app.post("/execute_delete")
async def execute_delete(
    request: Request,
    source_table_name: str = Form(...),
    target_table_name: str = Form(...),
    column_name: str = Form(...),
    condition: str = Form(...),
    archived_count: int = Form(...)
):
    """Show confirmation page before executing delete"""
    global source_db
    
    if not source_db:
        raise HTTPException(status_code=400, detail="No source database connection")
    
    try:
        # Get a preview of rows that will be deleted (first 5 rows)
        preview_query = f"SELECT * FROM {source_table_name} WHERE {column_name} {condition} LIMIT 5"
        preview_rows = source_db.execute_query(preview_query)
        
        # Get current count to verify it matches the archived count
        count_query = f"SELECT COUNT(*) as count FROM {source_table_name} WHERE {column_name} {condition}"
        count_result = source_db.execute_query(count_query)
        current_count = count_result[0]['count'] if count_result else 0
        
        delete_query = f"DELETE FROM {source_table_name} WHERE {column_name} {condition}"
        
        return templates.TemplateResponse("confirm_delete.html", {
            "request": request,
            "source_table_name": source_table_name,
            "target_table_name": target_table_name,
            "column_name": column_name,
            "condition": condition,
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
    column_name: str = Form(...),
    condition: str = Form(...),
    archived_count: int = Form(...)
):
    """Actually execute the DELETE query after confirmation"""
    global source_db
    
    if not source_db:
        raise HTTPException(status_code=400, detail="No source database connection")
    
    try:
        # Execute the DELETE query
        delete_query = f"DELETE FROM {source_table_name} WHERE {column_name} {condition}"
        deleted_count = source_db.execute_query(delete_query)
        
        return templates.TemplateResponse("delete_result.html", {
            "request": request,
            "success": True,
            "message": f"Successfully deleted {deleted_count} rows from source table",
            "deleted_count": deleted_count,
            "archived_count": archived_count,
            "source_table_name": source_table_name,
            "target_table_name": target_table_name,
            "column_name": column_name,
            "condition": condition,
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
            "column_name": column_name,
            "condition": condition,
            "delete_query": f"DELETE FROM {source_table_name} WHERE {column_name} {condition}"
        })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
