# Data Matrix

A Go service that loads CSV files into an in-memory DuckDB database and provides an HTTP API for querying the data using SQL. The service includes effective date tracking for column values and a full trie-based file structure for data persistence.

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

### Configuration Options

You can configure DataMatrix using either a JSON configuration file or environment variables.

#### JSON Configuration File

Create a `config.json` file in the application directory or specify a custom path using the `CONFIG_FILE` environment variable:

```bash
export CONFIG_FILE="/path/to/your/config.json"
```

Example configuration file:

```json
{
  "s3_bucket": "your-bucket-name",
  "s3_prefix": "data/",
  "data_dir": "data",
  "dir_whitelist": ["financial", "company", "market"],
  "id_prefix_filter": ["BBG", "ISIN"]
}
```

Configuration options:

| Option | Description |
|--------|-------------|
| `s3_bucket` | S3 bucket name |
| `s3_prefix` | Optional prefix/path within the bucket |
| `data_dir` | Directory for downloaded files (default: "data") |
| `dir_whitelist` | Optional list of directory patterns to include |
| `id_prefix_filter` | Optional list of ID_BB_GLOBAL patterns to include |

#### Environment Variables

If no configuration file is found, you can use environment variables:

```bash
# S3 bucket name
export S3_BUCKET="your-bucket-name"

# Directory whitelist - only process directories matching these patterns
export DIR_WHITELIST="equity,^bond/.*,fx$"

# ID_BB_GLOBAL prefix filter - only include IDs matching these patterns
export ID_PREFIX_FILTER="BBG00,^US\d+,.*EQUITY$"
```

#### Command Line Arguments

The application supports the following command line arguments:

```bash
# To skip both downloading and processing files (use existing data on disk)
go run main.go --skip-loading

# To skip downloading from S3 but still process local files
go run main.go --skip-downloading
```

These arguments are useful when:
- You want to quickly start the server without reloading all data (faster startup)
- You want to query existing assets directly from disk without refreshing the data
- You've already processed a large dataset and just want to serve it via the API

#### Starting the Server

After configuring with either method, start the server:

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

## Effective Date Tracking

The application maintains an index of effective dates for each column value to ensure data freshness and prevent overwriting newer data with older data.

### How Effective Date Tracking Works

1. **Date Extraction**: When loading a CSV file, the system extracts a date in YYYYMMDD format from the filename.
   - For example, from `financial_data_20250410.csv`, it extracts `20250410` as the effective date.
   - If no date is found in the filename, the current date is used as a fallback.

2. **Column-Level Tracking**: For each ID_BB_GLOBAL and column combination, the system tracks the effective date of the data.

3. **Update Logic**: When new data is loaded:
   - If a column doesn't exist yet for an ID, the value is added with the current file's effective date
   - If a column already exists, the value is only updated if the new file's effective date is newer than the existing one

4. **Persistence**: The effective date index is stored in `data/asset_index.json` and persists between application runs.

### Benefits

- Prevents older data from overwriting newer data
- Allows loading files in any order without data quality concerns
- Provides an audit trail of when data was last updated
- Enables incremental updates without full reloads

## Trie Directory Structure

The application uses a full trie directory structure to store JSON asset files efficiently:

### How the Trie Structure Works

1. **Full Path Utilization**: Every character in the ID_BB_GLOBAL is used to create the directory path.
   - For example, an ID like `BBG000B9XRY4` creates a directory structure: `json/b/b/g/0/0/0/b/9/x/r/y/4/BBG000B9XRY4.json`

2. **Benefits**:
   - Distributes files evenly across the filesystem
   - Prevents directories from containing too many files
   - Enables efficient lookups without scanning large directories
   - Scales well to millions of unique IDs

3. **Implementation**: The trie structure is automatically created when saving or accessing JSON files.

## Progress Tracking

The application includes a comprehensive progress tracking system to monitor file processing, row enumeration, and system status:

### Key Features

1. **S3 File Processing Progress**: Tracks the progress of listing, downloading, and processing files from S3.

2. **Row Enumeration**: Monitors the progress of processing rows within each CSV file, showing counts and percentages.

3. **Idle Status Detection**: Automatically detects when the system is idle and tracks idle duration.

4. **Progress Bar Visualization**: Provides a visual representation of progress for operations with known total counts.

5. **API Access**: Exposes progress information through the `/api/progress` endpoint for integration with monitoring tools or frontends.

### Implementation Details

- Progress updates are displayed in the console logs during processing.
- The system automatically transitions to an idle state after 5 seconds of inactivity.
- Progress tracking is thread-safe and can handle concurrent operations.
- Status updates include both numeric progress (current/total) and percentage completion.

## How It Works

The application:
1. Loads data from one of two sources:
   - Local CSV files from the `example-data` directory and its subdirectories (up to 2 levels deep)
   - An S3 bucket (when `S3_BUCKET` environment variable is set), downloading only the most recent file from each directory
2. Skips files without an `ID_BB_GLOBAL` column
3. Creates a wide table with one row per unique `ID_BB_GLOBAL` value
4. Maintains an index of column effective dates to ensure data freshness
5. Stores data in JSON files using a full trie directory structure based on ID_BB_GLOBAL
6. Keeps all data in an in-memory DuckDB database for fast SQL querying
7. Tracks progress of file processing and row enumeration with visual indicators
8. Exposes a REST API for querying the data and monitoring progress

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

### GET /api/index
Returns information about the effective date index.

Response:
```json
{
  "total_entries": 1250,
  "unique_ids": 150,
  "unique_columns": 35,
  "index_file": "data/asset_index.json"
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

### GET /api/asset/{id}
Returns the full JSON object for a specific asset by its ID_BB_GLOBAL.

Parameters:
- `id` (path): The ID_BB_GLOBAL of the asset to retrieve

Response:
```json
{
  "ID_BB_GLOBAL": "BBG000B9XRY4",
  "Company": "Apple Inc.",
  "Revenue": "365.8",
  "Industry": "Technology",
  "MarketCap": "2.5T"
}
```

### GET /api/asset/{id}/columns
Returns metadata about the columns for a specific asset, including effective dates and source files.

Parameters:
- `id` (path): The ID_BB_GLOBAL of the asset to retrieve column metadata for

Response:
```json
{
  "Company": {
    "effective_date": "20250410",
    "source_file": "financial_data_20250410.csv"
  },
  "Revenue": {
    "effective_date": "20250410",
    "source_file": "financial_data_20250410.csv"
  },
  "Industry": {
    "effective_date": "20250408",
    "source_file": "industry_data_20250408.csv"
  }
}
```

### GET /api/asset/{id}/select
Returns only the specified columns from an asset.

Parameters:
- `id` (path): The ID_BB_GLOBAL of the asset to retrieve
- `columns` (query): Comma-separated list of column names to return

Example: `/api/asset/BBG000B9XRY4/select?columns=Company,Revenue`

Response:
```json
{
  "ID_BB_GLOBAL": "BBG000B9XRY4",
  "Company": "Apple Inc.",
  "Revenue": "365.8"
}
```

### GET /api/progress
Returns the current progress status of file processing, row enumeration, and idle status.

Response:
```json
{
  "status": "Loading CSV files",
  "current": 3,
  "total": 5,
  "percentage": 60,
  "progress_bar": "[==================          ]",
  "is_idle": false,
  "display_string": "[==================          ] Loading CSV files (3/5) 60%"
}
```

When the system is idle:
```json
{
  "status": "Idle",
  "current": 0,
  "total": 0,
  "percentage": 0,
  "progress_bar": "[==============================]",
  "is_idle": true,
  "idle_time_seconds": 125.5,
  "idle_time_formatted": "2m5s",
  "display_string": "Status: Idle (for 2m5s)"
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

