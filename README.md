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

### Local Files
1. Place your CSV files in the `example-data` directory (sample data will be created automatically if the directory doesn't exist)
2. Start the server:
   ```bash
   go run main.go
   ```

### Loading from S3
You can also load data directly from an S3 bucket:

1. Set the S3 bucket name as an environment variable:
   ```bash
   export S3_BUCKET="your-bucket-name"
   ```

2. Ensure AWS credentials are properly configured (via environment variables, credentials file, or IAM role)

3. Optionally, configure directory whitelist and ID_BB_GLOBAL prefix filters:
   ```bash
   # Directory whitelist - only process directories matching these patterns
   export DIR_WHITELIST="equity,^bond/.*,fx$"
   
   # ID_BB_GLOBAL prefix filter - only include IDs matching these patterns
   export ID_PREFIX_FILTER="BBG00,^US\d+,.*EQUITY$"
   ```

4. Start the server:
   ```bash
   go run main.go
   ```

When using S3 integration:
- The application will traverse the bucket and find all CSV files (both plain and gzipped)
- For each directory in the bucket, it will download only the most recent CSV file
- Files are downloaded to a local `data` directory, preserving the original S3 directory structure
- Gzipped CSV files (`.csv.gz` or `.gz`) are read directly without decompression
- Only files with an `ID_BB_GLOBAL` column will be included in the final data matrix

#### Directory Whitelist and ID Filtering

You can control which data gets loaded using two filtering mechanisms:

1. **Directory Whitelist**: Specify which S3 directories to process using regex patterns or simple strings.
   - Set via the `DIR_WHITELIST` environment variable as a comma-separated list
   - If not specified, all directories will be processed

2. **ID_BB_GLOBAL Prefix Filter**: Specify which ID_BB_GLOBAL values to include using regex patterns or simple prefixes.
   - Set via the `ID_PREFIX_FILTER` environment variable as a comma-separated list
   - If not specified, all ID_BB_GLOBAL values will be included

##### Directory Whitelist Examples

1. **Include only directories with "equity" in the name**:
   ```bash
   export DIR_WHITELIST="equity"
   ```
   This will match directories like `equity`, `equity/us`, `global/equity`, etc.

2. **Include everything except directories with "bulk" in the name**:
   ```bash
   export DIR_WHITELIST="^((?!bulk).)*$"
   ```
   This negative lookahead regex will match any directory that doesn't contain "bulk".

3. **Include multiple specific patterns**:
   ```bash
   export DIR_WHITELIST="equity,^bond/.*,fx$"
   ```
   This will include directories containing "equity", starting with "bond/", or ending with "fx".

4. **Include only top-level directories (no subdirectories)**:
   ```bash
   export DIR_WHITELIST="^[^/]+$"
   ```
   This will match directories that don't contain a forward slash.

##### ID_BB_GLOBAL Prefix Filter Examples

1. **Include only Bloomberg IDs starting with BBG0**:
   ```bash
   export ID_PREFIX_FILTER="BBG0"
   ```
   This will match IDs like `BBG000B9XRY4`, `BBG00DL8NMV2`, etc.

2. **Include all BBG IDs with wildcard**:
   ```bash
   export ID_PREFIX_FILTER="^BBG.*"
   ```
   This regex will match any ID starting with "BBG".

3. **Include only equity securities**:
   ```bash
   export ID_PREFIX_FILTER=".*EQUITY$"
   ```
   This will match IDs ending with "EQUITY".

4. **Include multiple ID types**:
   ```bash
   export ID_PREFIX_FILTER="BBG0,^US\d+,.*EQUITY$"
   ```
   This will include IDs starting with "BBG0", matching the pattern "US" followed by digits, or ending with "EQUITY".

## How It Works

The application:
1. Loads data from one of two sources:
   - Local CSV files from the `example-data` directory and its subdirectories (up to 2 levels deep)
   - An S3 bucket (when `S3_BUCKET` environment variable is set), downloading only the most recent file from each directory
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
1. Uses a custom in-memory data dictionary for SQL-like querying
2. Automatically loads and merges CSV files from the example-data directory and its subdirectories (up to 2 levels deep)
3. Skips files without an ID_BB_GLOBAL column
4. Creates a wide table with one row per ID_BB_GLOBAL
5. Provides a REST API for querying the data with a minimal SQL dialect
6. Thread-safe access to the data
7. Supports directory whitelist filtering for S3 loading
8. Supports ID_BB_GLOBAL prefix filtering
9. Case-insensitive column names in queries (e.g., "revenue", "REVENUE", and "Revenue" all work)

## Cross-Compilation

### Building for Linux AMD64 (Amazon Linux 2)

To cross-compile the application for Linux AMD64 (suitable for Amazon Linux 2 AMI), follow these steps:

1. Set the necessary environment variables for cross-compilation:

```bash
export GOOS=linux
export GOARCH=amd64
export CGO_ENABLED=0  # Disable CGO for pure Go compilation
```

2. Build the application:

```bash
go build -o datamatrix-linux-amd64 .
```

3. Make the binary executable (if needed):

```bash
chmod +x datamatrix-linux-amd64
```

4. Transfer the binary to your Amazon Linux 2 instance:

```bash
scp datamatrix-linux-amd64 ec2-user@your-instance-ip:/path/to/destination/
```

5. On the Amazon Linux 2 instance, run the application:

```bash
./datamatrix-linux-amd64
```

Note: Since the application uses pure Go without CGO dependencies, it can be easily cross-compiled for different platforms without additional dependencies.

