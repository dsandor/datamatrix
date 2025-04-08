package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gorilla/mux"
	_ "github.com/marcboeker/go-duckdb"
	httpSwagger "github.com/swaggo/http-swagger"
	_ "datamatrix/docs" // Import generated docs
)

// DataMatrix manages the in-memory DuckDB instance
type DataMatrix struct {
	sync.RWMutex
	db      *sql.DB
	columns []string
	logger  *Logger
	s3Bucket string // S3 bucket name (optional)
	s3Prefix string // S3 prefix/path within the bucket (optional)
	dataDir  string // Local directory for downloaded S3 files
}

// DataMatrixConfig holds configuration for DataMatrix initialization
type DataMatrixConfig struct {
	S3Bucket string // Optional S3 bucket name
	S3Prefix string // Optional S3 prefix/path within the bucket
	DataDir  string // Directory for downloaded S3 files (default: "data")
}

func NewDataMatrix(config *DataMatrixConfig) (*DataMatrix, error) {
	// Create a logger
	logger := NewLogger()
	logger.Info("Initializing DataMatrix...")

	// Log initial memory usage
	logger.Memory("Initial memory usage: %s", GetMemoryUsageSummary())

	// Open an in-memory DuckDB database
	db, err := sql.Open("duckdb", "")
	if err != nil {
		logger.Error("Error opening DuckDB: %v", err)
		return nil, fmt.Errorf("error opening DuckDB: %v", err)
	}

	// Set default data directory if not specified
	dataDir := "data"
	if config != nil && config.DataDir != "" {
		dataDir = config.DataDir
	}

	dm := &DataMatrix{
		db:       db,
		logger:   logger,
		s3Bucket: config.S3Bucket,
		s3Prefix: config.S3Prefix,
		dataDir:  dataDir,
	}

	if err := dm.loadData(); err != nil {
		logger.Error("Error loading data: %v", err)
		db.Close()
		return nil, err
	}

	// Log memory usage after loading data
	logger.Memory("Memory usage after loading data: %s", GetMemoryUsageSummary())
	logger.Success("DataMatrix initialized successfully")

	return dm, nil
}

// findCSVFiles recursively finds CSV files up to maxDepth levels deep
func findCSVFiles(baseDir string, currentDepth, maxDepth int, logger *Logger) ([]string, error) {
	if currentDepth > maxDepth {
		return nil, nil
	}

	files, err := os.ReadDir(baseDir)
	if err != nil {
		logger.Error("Error reading directory %s: %v", baseDir, err)
		return nil, fmt.Errorf("error reading directory %s: %v", baseDir, err)
	}

	var csvFiles []string
	for _, file := range files {
		path := filepath.Join(baseDir, file.Name())
		if file.IsDir() {
			logger.Debug("Searching subdirectory: %s (depth: %d/%d)", path, currentDepth+1, maxDepth)
			// Recursively search subdirectories up to maxDepth
			subFiles, err := findCSVFiles(path, currentDepth+1, maxDepth, logger)
			if err != nil {
				logger.Warn("Warning: %v", err)
				continue
			}
			csvFiles = append(csvFiles, subFiles...)
		} else if strings.HasSuffix(strings.ToLower(file.Name()), ".csv") {
			logger.Debug("Found CSV file: %s", path)
			csvFiles = append(csvFiles, path)
		}
	}

	return csvFiles, nil
}

func (dm *DataMatrix) loadData() error {
	var csvFiles []string
	var err error

	// Check if we should load from S3
	if dm.s3Bucket != "" {
		if dm.s3Prefix != "" {
			dm.logger.Info("Loading data from S3 bucket: %s with prefix: %s", dm.s3Bucket, dm.s3Prefix)
		} else {
			dm.logger.Info("Loading data from S3 bucket: %s", dm.s3Bucket)
		}
		
		// Try to load from S3
		s3Files, s3Err := CopyS3FilesToLocal(dm.logger, dm.s3Bucket, dm.s3Prefix, dm.dataDir)
		if s3Err == nil {
			// S3 loading succeeded
			csvFiles = s3Files
			
			if dm.s3Prefix != "" {
				dm.logger.Success("Successfully loaded %d files from S3 bucket %s with prefix %s", len(csvFiles), dm.s3Bucket, dm.s3Prefix)
			} else {
				dm.logger.Success("Successfully loaded %d files from S3 bucket %s", len(csvFiles), dm.s3Bucket)
			}
		} else {
			// S3 loading failed, fall back to local data
			dm.logger.Warn("Error loading data from S3: %v", s3Err)
			dm.logger.Warn("Falling back to local data loading...")
		}
	} else {
		// Load from local filesystem
		dm.logger.Info("Searching for CSV files in example-data directory and subdirectories (up to 2 levels deep)...")
		csvFiles, err = findCSVFiles("example-data", 0, 2, dm.logger)
		if err != nil {
			dm.logger.Error("Error finding CSV files: %v", err)
			return fmt.Errorf("error finding CSV files: %v", err)
		}

		dm.logger.Success("Found %d CSV files in example-data directory and subdirectories (up to 2 levels deep)", len(csvFiles))
	}

	// Create temporary views for each CSV file and collect column information
	dm.logger.Info("Creating temporary views for CSV files...")
	validFiles := make([]string, 0)
	for _, filePath := range csvFiles {
		// Create a unique view name based on the file path
		relPath, err := filepath.Rel("example-data", filePath)
		if err != nil {
			dm.logger.Error("Error getting relative path for %s: %v", filePath, err)
			continue
		}
		
		// Create a safe view name by replacing non-alphanumeric characters
		viewName := "temp_" + strings.ReplaceAll(
			strings.ReplaceAll(
				strings.ReplaceAll(relPath, "/", "_"),
				"\\", "_"),
			".", "_")
		
		// Create a temporary view for the CSV (handles both regular and gzipped CSVs)
		dm.logger.Debug("Creating view for %s as %s", filePath, viewName)
		
		// DuckDB can automatically detect and handle gzipped files
		_, err = dm.db.Exec(fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM read_csv_auto('%s')", viewName, filePath))
		if err != nil {
			dm.logger.Error("Error creating view for %s: %v", filePath, err)
			continue
		}

		// Check if ID_BB_GLOBAL exists in this file
		var hasIDColumn bool
		row := dm.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) > 0 FROM pragma_table_info('%s') WHERE name = 'ID_BB_GLOBAL'", viewName))
		err = row.Scan(&hasIDColumn)
		if err != nil {
			dm.logger.Error("Error checking for ID_BB_GLOBAL in %s: %v", filePath, err)
			dm.db.Exec(fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName))
			continue
		}

		if !hasIDColumn {
			dm.logger.Warn("Skipping file %s: No ID_BB_GLOBAL column found", filePath)
			dm.db.Exec(fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName))
			continue
		}

		validFiles = append(validFiles, viewName)
		dm.logger.Success("Added valid CSV file: %s", filePath)
	}

	if len(validFiles) == 0 {
		return fmt.Errorf("no valid files with ID_BB_GLOBAL column found")
	}

	// Create the final aggregated query
	query := "WITH all_ids AS (SELECT DISTINCT ID_BB_GLOBAL FROM ("
	for i, view := range validFiles {
		if i > 0 {
			query += " UNION "
		}
		query += fmt.Sprintf("SELECT ID_BB_GLOBAL FROM %s", view)
	}
	query += "))\nSELECT all_ids.ID_BB_GLOBAL"

	// Add columns from each valid file
	columnMap := make(map[string]bool)
	columnMap["ID_BB_GLOBAL"] = true

	for _, view := range validFiles {
		rows, err := dm.db.Query(fmt.Sprintf("SELECT name FROM pragma_table_info('%s') WHERE name != 'ID_BB_GLOBAL'", view))
		if err != nil {
			dm.logger.Error("Error getting columns for %s: %v", view, err)
			continue
		}

		for rows.Next() {
			var colName string
			if err := rows.Scan(&colName); err != nil {
				rows.Close()
				dm.logger.Error("Error scanning column name: %v", err)
				continue
			}
			if !columnMap[colName] {
				columnMap[colName] = true
				query += fmt.Sprintf(",\n\tMAX(%s.%s) as %s", view, colName, colName)
			}
		}
		rows.Close()
	}

	query += "\nFROM all_ids"
	for _, view := range validFiles {
		query += fmt.Sprintf("\nLEFT JOIN %s ON all_ids.ID_BB_GLOBAL = %s.ID_BB_GLOBAL", view, view)
	}
	query += "\nGROUP BY all_ids.ID_BB_GLOBAL"

	// Create the final in-memory table
	_, err = dm.db.Exec("CREATE TABLE data_matrix AS " + query)
	if err != nil {
		return fmt.Errorf("error creating final table: %v", err)
	}

	// Clean up temporary views
	for _, view := range validFiles {
		dm.db.Exec(fmt.Sprintf("DROP VIEW IF EXISTS %s", view))
	}

	// Get column information
	rows, err := dm.db.Query("SELECT name FROM pragma_table_info('data_matrix')")
	if err != nil {
		return fmt.Errorf("error getting columns: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return fmt.Errorf("error scanning column name: %v", err)
		}
		dm.columns = append(dm.columns, colName)
	}

	// Get row count
	var rowCount int64
	row := dm.db.QueryRow("SELECT COUNT(*) FROM data_matrix")
	err = row.Scan(&rowCount)
	if err != nil {
		return fmt.Errorf("error counting rows: %v", err)
	}

	dm.logger.Success("Loaded data_matrix table with %d rows and %d columns", rowCount, len(dm.columns))
	return nil
}

func (dm *DataMatrix) Close() error {
	dm.logger.Info("Closing database connection...")
	err := dm.db.Close()
	if err != nil {
		dm.logger.Error("Error closing database: %v", err)
	} else {
		dm.logger.Success("Database closed successfully")
	}
	return err
}

// @Summary Get all available columns
// @Description Returns the list of all columns available in the data_matrix table
// @Tags columns
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /api/columns [get]
func (dm *DataMatrix) handleGetColumns(w http.ResponseWriter, r *http.Request) {
	dm.RLock()
	defer dm.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"columns": dm.columns,
		"count":   len(dm.columns),
	})
}

// QueryRequest defines the structure for the query API request
type QueryRequest struct {
	// Optional list of columns to return. If empty or omitted, all columns will be returned (equivalent to SELECT *)
	// To select all columns, you can either: 1) omit this field, 2) provide an empty array, or 3) use ["*"]
	// Column names are case-insensitive, so you can use "revenue", "REVENUE", or "Revenue" interchangeably
	Columns []string `json:"columns" example:"[\"ID_BB_GLOBAL\",\"Company\",\"Revenue\"]"` 

	// Optional SQL WHERE clause to filter results (e.g., "Revenue > 200 AND Industry = 'Technology'")
	Where   string   `json:"where,omitempty" example:"Revenue > 200"`

	// Optional limit for the number of results to return
	Limit   int      `json:"limit,omitempty" example:"10"`

	// Optional offset for pagination
	Offset  int      `json:"offset,omitempty" example:"0"`
}

// QueryResponse defines the structure for the query API response
type QueryResponse struct {
	Data  []map[string]interface{} `json:"data"`  // The query results
	Count int                      `json:"count"` // Number of results returned
	Total int64                    `json:"total"` // Total number of records in the database
}

// @Summary Query the data_matrix table
// @Description Execute a SQL query against the data_matrix table with optional filtering and pagination
// @Description To select all columns (equivalent to SELECT * FROM data_matrix), you can either:
// @Description 1) Omit the columns field entirely
// @Description 2) Set columns to an empty array
// @Description 3) Explicitly use ["*"] as the columns value
// @Description All three approaches will return all columns for the matching rows.
// @Description Column names are case-insensitive, so you can use "revenue", "REVENUE", or "Revenue" interchangeably.
// @Tags query
// @Accept json
// @Produce json
// @Param query body QueryRequest true "Query parameters"
// @Success 200 {object} QueryResponse
// @Failure 400 {string} string "Invalid request body"
// @Failure 500 {string} string "Query error"
// @Router /api/query [post]
func (dm *DataMatrix) handleQuery(w http.ResponseWriter, r *http.Request) {
	var params QueryRequest

	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	dm.RLock()
	defer dm.RUnlock()

	// If no columns specified, return all columns
	if len(params.Columns) == 0 {
		params.Columns = []string{"*"}
	}

	// Construct SQL query with case-insensitive column handling
	var columnList string
	if len(params.Columns) == 1 && params.Columns[0] == "*" {
		// If selecting all columns, just use *
		columnList = "*"
	} else {
		// Otherwise, make each column name case-insensitive using ILIKE
		columnParts := make([]string, len(params.Columns))
		for i, col := range params.Columns {
			// For each column, find the actual column name with correct case
			columnParts[i] = fmt.Sprintf("CASE WHEN '%s' ILIKE 'id_bb_global' THEN ID_BB_GLOBAL ELSE "+
				"(SELECT CASE WHEN COUNT(*) > 0 THEN MAX("+
				"CASE WHEN LOWER(column_name) = LOWER('%s') THEN column_name END)"+
				" ELSE '%s' END FROM pragma_table_info('data_matrix') WHERE LOWER(name) = LOWER('%s')) END", 
				col, col, col, col)
		}
		columnList = strings.Join(columnParts, ", ")
	}

	query := fmt.Sprintf("SELECT %s FROM data_matrix", columnList)
	if params.Where != "" {
		query += " WHERE " + params.Where
	}
	if params.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", params.Limit)
	}
	if params.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", params.Offset)
	}

	// Execute query
	rows, err := dm.db.Query(query)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Get column names from the query result
	columns, err := rows.Columns()
	if err != nil {
		http.Error(w, fmt.Sprintf("Error getting columns: %v", err), http.StatusInternalServerError)
		return
	}

	// Prepare result
	var result []map[string]interface{}
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	for rows.Next() {
		err := rows.Scan(valuePtrs...)
		if err != nil {
			http.Error(w, fmt.Sprintf("Error scanning row: %v", err), http.StatusInternalServerError)
			return
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		result = append(result, row)
	}

	// Get total count
	var total int64
	row := dm.db.QueryRow("SELECT COUNT(*) FROM data_matrix")
	err = row.Scan(&total)
	if err != nil {
		dm.logger.Error("Error getting total count: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(QueryResponse{
		Data:  result,
		Count: len(result),
		Total: total,
	})
}

// @title DataMatrix API
// @version 1.0
// @description A Go service that loads CSV files into an in-memory DuckDB database and provides an HTTP API for querying the data using SQL.
// @host localhost:8080
// @BasePath /
func main() {
	// Create a logger for the main function
	logger := NewLogger()
	
	// Check if example-data directory exists, if not create test data
	if _, err := os.Stat("example-data"); os.IsNotExist(err) {
		logger.Info("Creating test data...")
		if err := createTestData(); err != nil {
			logger.Error("Error creating test data: %v", err)
			os.Exit(1)
		}
	}

	// Create the DataMatrix configuration
	config := &DataMatrixConfig{}
	
	// Check if S3 bucket is specified as an environment variable
	s3Path := os.Getenv("S3_BUCKET")
	if s3Path != "" {
		// Remove the s3:// prefix if present
		s3Path = strings.TrimPrefix(s3Path, "s3://")
		
		// Split the path into bucket and prefix
		parts := strings.SplitN(s3Path, "/", 2)
		bucketName := parts[0]
		prefix := ""
		if len(parts) > 1 {
			prefix = parts[1]
		}
		
		logger.Info("S3 bucket specified: %s, prefix: %s", bucketName, prefix)
		config.S3Bucket = bucketName
		config.S3Prefix = prefix
		config.DataDir = "data" // Default data directory for S3 downloads
	}
	
	// Create the DataMatrix
	dm, err := NewDataMatrix(config)
	if err != nil {
		logger.Error("Error initializing DataMatrix: %v", err)
		os.Exit(1)
	}
	defer dm.Close()

	r := mux.NewRouter()
	
	// API endpoints
	r.HandleFunc("/api/columns", dm.handleGetColumns).Methods("GET")
	r.HandleFunc("/api/query", dm.handleQuery).Methods("POST")
	
	// Serve Swagger UI at root
	r.PathPrefix("/swagger/").Handler(httpSwagger.WrapHandler)
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/swagger/index.html", http.StatusMovedPermanently)
	})

	port := "8080"
	logger.Info("Starting server on port %s", port)
	logger.Info("Swagger UI available at http://localhost:%s/swagger/index.html", port)
	
	// Log memory usage before starting server
	logger.Memory("Memory usage before starting server: %s", GetMemoryUsageSummary())
	
	if err := http.ListenAndServe(":"+port, r); err != nil {
		logger.Error("Error starting server: %v", err)
		os.Exit(1)
	}
}
