# PostgreSQL Database Archiver

A FastAPI web application for archiving data between PostgreSQL databases. This tool allows you to safely move data from a source database to a target database based on specified conditions.

## Features

- **Dual Database Connection**: Connect to separate source and target PostgreSQL databases
- **Table Discovery**: Automatically lists all tables in the source database
- **Column Inspection**: View column information including data types and constraints
- **Flexible Filtering**: Create custom conditions using any column and SQL operators
- **Data Preview**: Preview matching rows before archiving
- **Safe Archiving**: Copy data to target database first, then provide DELETE query for manual cleanup
- **Table Creation**: Automatically creates target tables with the same structure if they don't exist

## Prerequisites

- Python 3.7+
- PostgreSQL databases (source and target)
- Network access to both databases

## Installation

1. Clone this repository:
```bash
git clone <repository-url>
cd postgres-archiver
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

1. Start the application:
```bash
python main.py
```

2. Open your web browser and navigate to `http://localhost:8000`

3. Follow the web interface steps:
   - **Step 1**: Enter connection details for source and target databases
   - **Step 2**: Select a table from the source database
   - **Step 3**: Choose a column and specify a condition for filtering
   - **Step 4**: Preview the data that will be archived
   - **Step 5**: Execute the archive operation
   - **Step 6**: Manually execute the provided DELETE query on the source database

## Example Conditions

- Date-based: `< '2023-01-01'`
- Status-based: `= 'archived'`
- Multiple values: `IN ('status1', 'status2')`
- Null values: `IS NULL`
- Numeric: `> 100`

## Safety Features

- **Two-step process**: Data is copied first, then you manually delete from source
- **Preview functionality**: See exactly what data will be archived
- **Confirmation prompts**: Prevent accidental operations
- **Copy-to-clipboard**: Easy copying of DELETE queries
- **Transaction safety**: Operations are wrapped in database transactions

## Database Permissions Required

The application requires the following permissions:

**Source Database**:
- `SELECT` on tables to be archived
- `SELECT` on `information_schema` tables

**Target Database**:
- `CREATE TABLE` (if tables don't exist)
- `INSERT` on target tables

## Security Considerations

- Always test with non-production data first
- Ensure you have database backups before archiving
- Use read-only credentials for source database when possible
- Verify DELETE queries before execution
- Consider using database transactions for large operations

## Architecture

The application consists of:
- **FastAPI backend**: Handles database connections and operations
- **Jinja2 templates**: Provides the web interface
- **psycopg2**: PostgreSQL database adapter
- **Responsive HTML/CSS**: Mobile-friendly interface

## API Endpoints

- `GET /`: Home page with database connection form
- `POST /connect`: Establish database connections and list tables
- `POST /select_table`: Get column information for selected table
- `POST /preview_data`: Preview data matching the condition
- `POST /archive_data`: Execute the archive operation

## Error Handling

The application includes comprehensive error handling for:
- Database connection failures
- Invalid SQL conditions
- Missing permissions
- Network timeouts
- Data type mismatches

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions, please create an issue in the GitHub repository.