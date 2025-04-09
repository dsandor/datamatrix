package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/gorilla/mux"
	httpSwagger "github.com/swaggo/http-swagger"
	_ "datamatrix/docs" // Import generated docs
)

// DataMatrix manages the JSON-based asset storage
type DataMatrix struct {
	sync.RWMutex
	assetManager   *JSONAssetManager
	logger         *Logger
	s3Bucket       string   // S3 bucket name (optional)
	s3Prefix       string   // S3 prefix/path within the bucket (optional)
	dataDir        string   // Local directory for downloaded S3 files
	dirWhitelist   []string // Optional whitelist of directory names
	idPrefixFilter []string // Optional ID_BB_GLOBAL prefix filter
}

// DataMatrixConfig holds configuration for DataMatrix initialization
type DataMatrixConfig struct {
	S3Bucket       string   `json:"s3_bucket,omitempty"`       // Optional S3 bucket name
	S3Prefix       string   `json:"s3_prefix,omitempty"`       // Optional S3 prefix/path within the bucket
	DataDir        string   `json:"data_dir,omitempty"`        // Directory for downloaded S3 files (default: "data")
	DirWhitelist   []string `json:"dir_whitelist,omitempty"`   // Optional whitelist of directory names
	IDPrefixFilter []string `json:"id_prefix_filter,omitempty"` // Optional ID_BB_GLOBAL prefix filter
	ConfigFile     string   `json:"-"`                         // Path to the configuration file (not stored in JSON)
}

func NewDataMatrix(config *DataMatrixConfig) (*DataMatrix, error) {
	// Create a logger
	logger := NewLogger()
	logger.Info("Initializing DataMatrix...")

	// Log initial memory usage
	logger.Memory("Initial memory usage: %s", GetMemoryUsageSummary())

	// Set default data directory if not specified
	dataDir := "data"
	if config != nil && config.DataDir != "" {
		dataDir = config.DataDir
	}
	
	// Create a new JSON asset manager
	assetManager, err := NewJSONAssetManager(logger, dataDir)
	if err != nil {
		logger.Error("Error creating JSON asset manager: %v", err)
		return nil, err
	}
	
	// Set ID prefix filter if specified
	if config != nil && len(config.IDPrefixFilter) > 0 {
		assetManager.SetIDPrefixFilter(config.IDPrefixFilter)
	}

	dm := &DataMatrix{
		assetManager:   assetManager,
		logger:         logger,
		s3Bucket:       config.S3Bucket,
		s3Prefix:       config.S3Prefix,
		dataDir:        dataDir,
		dirWhitelist:   config.DirWhitelist,
		idPrefixFilter: config.IDPrefixFilter,
	}

	if err := dm.loadData(); err != nil {
		logger.Error("Error loading data: %v", err)
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
		s3Files, s3Err := CopyS3FilesToLocal(dm.logger, dm.s3Bucket, dm.s3Prefix, dm.dataDir, dm.dirWhitelist, dm.idPrefixFilter)
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

	// Load the CSV files into our JSON asset store
	dm.logger.Info("Loading CSV files into JSON asset store...")
	
	// Load all CSV files into the JSON asset store
	err = dm.assetManager.LoadFiles(csvFiles)
	if err != nil {
		return fmt.Errorf("error loading CSV files into JSON asset store: %v", err)
	}
	
	// Success message - we don't need to check for empty data as files are stored on disk
	dm.logger.Success("Loaded CSV files into JSON asset store with %d columns", 
		len(dm.assetManager.GetColumns()))
	return nil
}

func (dm *DataMatrix) Close() error {
	// Nothing special to close with our file-based implementation
	dm.logger.Info("Closing DataMatrix...")
	dm.logger.Success("DataMatrix closed successfully")
	return nil
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
	columns := dm.assetManager.GetColumns()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"columns": columns,
		"count":   len(columns),
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

	// Build a SQL query string for our custom implementation
	sqlQuery := "SELECT " + columnList + " FROM BB_ASSETS"
	if params.Where != "" {
		sqlQuery += " WHERE " + params.Where
	}

	// Execute the query against our JSON asset store
	result, err := dm.assetManager.ExecuteSQLQuery(sqlQuery)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusInternalServerError)
		return
	}

	// For total count, we'll use the result count as an approximation
	// since we don't keep a full list of IDs in memory anymore
	total := int64(len(result))

	// Convert result from []map[string]string to []map[string]interface{}
	interfaceResult := make([]map[string]interface{}, len(result))
	for i, row := range result {
		interfaceRow := make(map[string]interface{})
		for k, v := range row {
			interfaceRow[k] = v
		}
		interfaceResult[i] = interfaceRow
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(QueryResponse{
		Data:  interfaceResult,
		Count: len(result),
		Total: total,
	})
}

// @title DataMatrix API
// @version 1.0
// @description A Go service that loads CSV files into a JSON-based file store and provides an HTTP API for querying the data using a minimal SQL dialect.
// @host localhost:8080
// @BasePath /

// loadConfigFromFile loads DataMatrix configuration from a JSON file
func loadConfigFromFile(filePath string, logger *Logger) (*DataMatrixConfig, error) {
	logger.Info("Loading configuration from file: %s", filePath)
	
	// Read the configuration file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %v", err)
	}
	
	// Parse the JSON configuration
	config := &DataMatrixConfig{}
	if err := json.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("error parsing config file: %v", err)
	}
	
	// Store the config file path
	config.ConfigFile = filePath
	
	// Set default data directory if not specified
	if config.DataDir == "" {
		config.DataDir = "data"
		logger.Info("No data directory specified in config, using default: %s", config.DataDir)
	}
	
	// Log the configuration
	if config.S3Bucket != "" {
		logger.Info("S3 bucket specified in config: %s, prefix: %s", config.S3Bucket, config.S3Prefix)
	}
	
	if len(config.DirWhitelist) > 0 {
		logger.Info("Directory whitelist specified with %d patterns", len(config.DirWhitelist))
		for _, pattern := range config.DirWhitelist {
			logger.Debug("Directory whitelist pattern: %s", pattern)
		}
	}
	
	if len(config.IDPrefixFilter) > 0 {
		logger.Info("ID_BB_GLOBAL prefix filter specified with %d patterns", len(config.IDPrefixFilter))
		for _, pattern := range config.IDPrefixFilter {
			logger.Debug("ID_BB_GLOBAL prefix pattern: %s", pattern)
		}
	}
	
	return config, nil
}

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
	var config *DataMatrixConfig
	
	// Check if config file is specified as an environment variable or use default
	configFile := os.Getenv("CONFIG_FILE")
	if configFile != "" {
		// Load configuration from file
		var err error
		config, err = loadConfigFromFile(configFile, logger)
		if err != nil {
			logger.Error("Error loading configuration from file: %v", err)
			os.Exit(1)
		}
		logger.Success("Configuration loaded from file: %s", configFile)
	} else {
		// Check if default config file exists
		defaultConfigFile := "config.json"
		if _, err := os.Stat(defaultConfigFile); err == nil {
			// Load configuration from default file
			var err error
			config, err = loadConfigFromFile(defaultConfigFile, logger)
			if err != nil {
				logger.Error("Error loading configuration from default file: %v", err)
				os.Exit(1)
			}
			logger.Success("Configuration loaded from default file: %s", defaultConfigFile)
		} else {
			// No config file, use environment variables
			logger.Info("No configuration file found, using environment variables")
			config = &DataMatrixConfig{}
			
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
				
				// Check for directory whitelist
				dirWhitelist := os.Getenv("DIR_WHITELIST")
				if dirWhitelist != "" {
					whitelistPatterns := strings.Split(dirWhitelist, ",")
					config.DirWhitelist = whitelistPatterns
					logger.Info("Directory whitelist specified with %d patterns", len(whitelistPatterns))
					for _, pattern := range whitelistPatterns {
						logger.Debug("Directory whitelist pattern: %s", pattern)
					}
				}
				
				// Check for ID_BB_GLOBAL prefix filter
				idPrefixFilter := os.Getenv("ID_PREFIX_FILTER")
				if idPrefixFilter != "" {
					prefixPatterns := strings.Split(idPrefixFilter, ",")
					config.IDPrefixFilter = prefixPatterns
					logger.Info("ID_BB_GLOBAL prefix filter specified with %d patterns", len(prefixPatterns))
					for _, pattern := range prefixPatterns {
						logger.Debug("ID_BB_GLOBAL prefix pattern: %s", pattern)
					}
				}
			}
		}
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
