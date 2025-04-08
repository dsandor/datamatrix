# Data Matrix

A Go service that loads CSV files into an in-memory DuckDB database and provides an HTTP API for querying the data using SQL.

## Requirements
- Go 1.24.2 or later
- DuckDB (automatically used via Go bindings)

## Installation
```bash
go mod tidy
```

## Usage
1. Place your CSV files in the `example-data` directory (sample data will be created automatically if the directory doesn't exist)
2. Start the server:
   ```bash
   go run main.go
   ```

## How It Works

The application:
1. Loads all CSV files from the `example-data` directory and its subdirectories (up to 2 levels deep)
2. Skips files without an `ID_BB_GLOBAL` column
3. Creates a wide table with one row per unique `ID_BB_GLOBAL` value
4. Keeps all data in an in-memory DuckDB database for fast SQL querying
5. Exposes a REST API for querying the data

## API Endpoints

### GET /api/columns
Returns the list of available columns in the data_matrix table.

Response:
```json
{
  "columns": ["ID_BB_GLOBAL", "Company", "Industry", "Revenue", "Employees", "Founded", "Headquarters"],
  "count": 7
}
```

### POST /api/query
Query the data_matrix table using SQL WHERE clauses.

Request body:
```json
{
  "columns": ["ID_BB_GLOBAL", "Company", "Revenue"],  // Optional, defaults to ["*"]
  "where": "Revenue > 200",                        // Optional SQL WHERE clause
  "limit": 10,                                      // Optional
  "offset": 0                                       // Optional
}
```

#### Using SELECT * Queries
To select all columns (equivalent to `SELECT * FROM data_matrix`), you can either:

1. **Omit the `columns` field entirely**:
```json
{
  "where": "Revenue > 200"
}
```

2. **Set `columns` to an empty array**:
```json
{
  "columns": [],
  "where": "Revenue > 200"
}
```

3. **Explicitly use `*` as a column**:
```json
{
  "columns": ["*"],
  "where": "Revenue > 200"
}
```

All three approaches will return all columns for the matching rows.

#### Case-Insensitive Column Names

Column names in queries are case-insensitive. For example, the following queries are all equivalent:

```json
{"columns": ["ID_BB_GLOBAL", "revenue", "company"]}
```

```json
{"columns": ["id_bb_global", "REVENUE", "Company"]}
```

```json
{"columns": ["Id_Bb_Global", "Revenue", "COMPANY"]}
```

This makes the API more user-friendly and less error-prone when working with column names.

Response:
```json
{
  "data": [
    {"ID_BB_GLOBAL": "AAPL", "Company": "Apple Inc.", "Revenue": 365.8},
    {"ID_BB_GLOBAL": "AMZN", "Company": "Amazon.com Inc.", "Revenue": 386.1}
  ],
  "count": 2,
  "total": 6
}
```

## Features
1. Uses DuckDB as the in-memory SQL query engine
2. Automatically loads and merges CSV files from the example-data directory and its subdirectories (up to 2 levels deep)
3. Skips files without an ID_BB_GLOBAL column
4. Creates a wide table with one row per ID_BB_GLOBAL
5. Provides a REST API for querying the data with SQL
6. Thread-safe access to the database
7. Supports full SQL querying capabilities through DuckDB
8. Case-insensitive column names in queries (e.g., "revenue", "REVENUE", and "Revenue" all work)

