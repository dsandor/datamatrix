package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	httpSwagger "github.com/swaggo/http-swagger"
	_ "datamatrix/docs" // Import generated docs
)

// DataMatrix manages the JSON-based asset storage
type DataMatrix struct {
	sync.RWMutex
	assetManager   *JSONAssetManager
	logger         *Logger
	progress       *ProgressTracker
	s3Bucket       string   // S3 bucket name (optional)
	s3Prefix       string   // S3 prefix/path within the bucket (optional)
	dataDir        string   // Local directory for downloaded S3 files
	dirWhitelist   []string // Optional whitelist of directory names
	idPrefixFilter []string // Optional ID_BB_GLOBAL prefix filter
	skipFileLoading bool    // Flag to skip file loading and downloading
}

// DataMatrixConfig holds configuration for DataMatrix initialization
type DataMatrixConfig struct {
	S3Bucket       string   `json:"s3_bucket,omitempty"`       // Optional S3 bucket name
	S3Prefix       string   `json:"s3_prefix,omitempty"`       // Optional S3 prefix/path within the bucket
	DataDir        string   `json:"data_dir,omitempty"`        // Directory for downloaded S3 files (default: "data")
	DirWhitelist   []string `json:"dir_whitelist,omitempty"`   // Optional whitelist of directory names
	IDPrefixFilter []string `json:"id_prefix_filter,omitempty"` // Optional ID_BB_GLOBAL prefix filter
	SkipFileLoading bool     `json:"skip_file_loading,omitempty"` // Flag to skip file loading and downloading
	ConfigFile     string   `json:"-"`                         // Path to the configuration file (not stored in JSON)
}

func NewDataMatrix(config *DataMatrixConfig) (*DataMatrix, error) {
	// Create a logger
	logger := NewLogger()
	logger.Info("Initializing DataMatrix...")

	// Create a progress tracker
	progress := NewProgressTracker(logger)
	
	// Log initial memory usage
	logger.Memory("Initial memory usage: %s", GetMemoryUsageSummary())

	// Set default data directory if not specified
	dataDir := "data"
	if config != nil && config.DataDir != "" {
		dataDir = config.DataDir
	}
	
	// Create a new JSON asset manager
	assetManager, err := NewJSONAssetManager(logger, progress, dataDir)
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
		progress:       progress,
		s3Bucket:       config.S3Bucket,
		s3Prefix:       config.S3Prefix,
		dataDir:        dataDir,
		dirWhitelist:   config.DirWhitelist,
		idPrefixFilter: config.IDPrefixFilter,
		skipFileLoading: config.SkipFileLoading,
	}

	// Only load data if not skipping file loading
	if !dm.skipFileLoading {
		if err := dm.loadData(); err != nil {
			logger.Error("Error loading data: %v", err)
			return nil, err
		}
	} else {
		logger.Info("Skipping file loading and downloading as requested")
		logger.Info("Will serve API using existing data from disk")
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

	// Check if we should load from S3 and if we're not skipping downloading
	if dm.s3Bucket != "" && !dm.skipFileLoading {
		if dm.s3Prefix != "" {
			dm.logger.Info("Loading data from S3 bucket: %s with prefix: %s", dm.s3Bucket, dm.s3Prefix)
		} else {
			dm.logger.Info("Loading data from S3 bucket: %s", dm.s3Bucket)
		}
		
		// Try to load from S3
		s3Files, s3Err := CopyS3FilesToLocal(dm.logger, dm.progress, dm.s3Bucket, dm.s3Prefix, dm.dataDir, dm.dirWhitelist, dm.idPrefixFilter)
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
	} else if !dm.skipFileLoading {
		// Load from local filesystem
		dm.logger.Info("Searching for CSV files in example-data directory and subdirectories (up to 2 levels deep)...")
		csvFiles, err = findCSVFiles("example-data", 0, 2, dm.logger)
		if err != nil {
			dm.logger.Error("Error finding CSV files: %v", err)
			return fmt.Errorf("error finding CSV files: %v", err)
		}

		dm.logger.Success("Found %d CSV files in example-data directory and subdirectories (up to 2 levels deep)", len(csvFiles))
	}

	// If we're skipping file loading, just scan existing assets
	if dm.skipFileLoading {
		dm.logger.Info("Skipping file loading, scanning existing assets on disk...")
		
		// Ensure the asset manager scans existing assets
		if err := dm.assetManager.scanExistingAssets(); err != nil {
			dm.logger.Warn("Error scanning existing assets: %v", err)
			return fmt.Errorf("error scanning existing assets: %v", err)
		}
		
		dm.logger.Success("Successfully loaded existing assets from disk with %d columns", 
			len(dm.assetManager.GetColumns()))
		return nil
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

// @Summary Get index information
// @Description Returns information about the asset index including effective dates
// @Tags index
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /api/index [get]
func (dm *DataMatrix) handleGetIndexInfo(w http.ResponseWriter, r *http.Request) {
	dm.RLock()
	defer dm.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	indexInfo := dm.assetManager.GetIndexInfo()
	json.NewEncoder(w).Encode(indexInfo)
}

// @Summary Get progress information
// @Description Returns the current progress status of file processing, row enumeration, and idle status
// @Tags progress
// @Produce json
// @Success 200 {object} map[string]interface{}
// @Router /api/progress [get]
func (dm *DataMatrix) handleGetProgress(w http.ResponseWriter, r *http.Request) {
	dm.RLock()
	defer dm.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	
	// Get the progress tracker's current status
	progressStr := dm.progress.GetProgressString()
	
	// Get additional progress information
	dm.progress.RLock()
	response := map[string]interface{}{
		"status": dm.progress.status,
		"current": dm.progress.current,
		"total": dm.progress.total,
		"percentage": dm.progress.percentage,
		"progress_bar": dm.progress.progressBar,
		"is_idle": dm.progress.isIdle,
		"display_string": progressStr,
	}
	
	// Add idle time if the system is idle
	if dm.progress.isIdle {
		idleTime := time.Since(dm.progress.idleStartTime).Seconds()
		response["idle_time_seconds"] = idleTime
		response["idle_time_formatted"] = time.Since(dm.progress.idleStartTime).Round(time.Second).String()
	}
	dm.progress.RUnlock()
	
	json.NewEncoder(w).Encode(response)
}

// @Summary Get asset by ID_BB_GLOBAL
// @Description Returns the full JSON object for a specific asset
// @Tags asset
// @Produce json
// @Param id path string true "ID_BB_GLOBAL of the asset"
// @Success 200 {object} map[string]string
// @Failure 404 {string} string "Asset not found"
// @Failure 500 {string} string "Internal server error"
// @Router /api/asset/{id} [get]
func (dm *DataMatrix) handleGetAsset(w http.ResponseWriter, r *http.Request) {
	dm.RLock()
	defer dm.RUnlock()
	
	// Get the asset ID from the URL parameters
	vars := mux.Vars(r)
	id := vars["id"]
	
	// Get the asset from the asset manager
	asset, err := dm.assetManager.GetAsset(id)
	if err != nil {
		// Check if the error is because the asset doesn't exist
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "Asset with ID %s not found", id)
			return
		}
		
		// Otherwise, it's an internal server error
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error retrieving asset: %v", err)
		return
	}
	
	// Return the asset as JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(asset)
}

// @Summary Get asset columns
// @Description Returns the columns and their metadata for a specific asset
// @Tags asset
// @Produce json
// @Param id path string true "ID_BB_GLOBAL of the asset"
// @Success 200 {object} map[string]map[string]string
// @Failure 404 {string} string "Asset not found"
// @Failure 500 {string} string "Internal server error"
// @Router /api/asset/{id}/columns [get]
func (dm *DataMatrix) handleGetAssetColumns(w http.ResponseWriter, r *http.Request) {
	dm.RLock()
	defer dm.RUnlock()
	
	// Get the asset ID from the URL parameters
	vars := mux.Vars(r)
	id := vars["id"]
	
	// Check if the asset exists
	_, err := dm.assetManager.GetAsset(id)
	if err != nil {
		// Check if the error is because the asset doesn't exist
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "Asset with ID %s not found", id)
			return
		}
		
		// Otherwise, it's an internal server error
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error checking asset existence: %v", err)
		return
	}
	
	// Get the column metadata for the asset
	columnMetadata, err := dm.assetManager.GetAssetColumnMetadata(id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error retrieving column metadata: %v", err)
		return
	}
	
	// Return the column metadata as JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(columnMetadata)
}

// @Summary Get specific columns from an asset
// @Description Returns only the specified columns from an asset
// @Tags asset
// @Produce json
// @Param id path string true "ID_BB_GLOBAL of the asset"
// @Param columns query string false "Comma-separated list of column names to return"
// @Success 200 {object} map[string]string
// @Failure 404 {string} string "Asset not found"
// @Failure 500 {string} string "Internal server error"
// @Router /api/asset/{id}/select [get]
func (dm *DataMatrix) handleGetAssetSelect(w http.ResponseWriter, r *http.Request) {
	dm.RLock()
	defer dm.RUnlock()
	
	// Get the asset ID from the URL parameters
	vars := mux.Vars(r)
	id := vars["id"]
	
	// Get the columns parameter from the query string
	columnsParam := r.URL.Query().Get("columns")
	
	// Get the asset from the asset manager
	asset, err := dm.assetManager.GetAsset(id)
	if err != nil {
		// Check if the error is because the asset doesn't exist
		if os.IsNotExist(err) {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "Asset with ID %s not found", id)
			return
		}
		
		// Otherwise, it's an internal server error
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Error retrieving asset: %v", err)
		return
	}
	
	// If no columns parameter is provided, return the full asset
	if columnsParam == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(asset)
		return
	}
	
	// Split the columns parameter by comma
	columns := strings.Split(columnsParam, ",")
	
	// Create a new map with only the requested columns
	result := make(map[string]string)
	
	// Always include the ID_BB_GLOBAL column
	result["ID_BB_GLOBAL"] = asset["ID_BB_GLOBAL"]
	
	// Add the requested columns if they exist in the asset
	for _, col := range columns {
		col = strings.TrimSpace(col)
		if val, ok := asset[col]; ok {
			result[col] = val
		}
	}
	
	// Return the filtered asset as JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
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
	// Parse command line flags
	skipFileLoading := flag.Bool("skip-loading", false, "Skip file loading and downloading, serve API using existing data on disk")
	skipDownloading := flag.Bool("skip-downloading", false, "Skip downloading files from S3, but still process local files")
	flag.Parse()
	
	// Create a logger for the main function
	logger := NewLogger()
	
	// Log command line flags
	if *skipFileLoading {
		logger.Info("Running with --skip-loading flag: Will skip file loading and downloading")
	}
	if *skipDownloading {
		logger.Info("Running with --skip-downloading flag: Will skip downloading files from S3")
	}
	
	// Check if example-data directory exists, if not create test data
	if _, err := os.Stat("example-data"); os.IsNotExist(err) && !*skipFileLoading {
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
			
			// Apply command line flags to config
			config.SkipFileLoading = *skipFileLoading
			
			// Handle skip-downloading flag - if we're skipping downloading but not skipping loading,
			// we'll still process local files
			if *skipDownloading && !*skipFileLoading {
				// Clear S3 bucket to prevent S3 loading
				config.S3Bucket = ""
				logger.Info("Skipping S3 downloading, will process local files only")
			}
			
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
	r.HandleFunc("/api/index", dm.handleGetIndexInfo).Methods("GET")
	r.HandleFunc("/api/query", dm.handleQuery).Methods("POST")
	r.HandleFunc("/api/progress", dm.handleGetProgress).Methods("GET")
	r.HandleFunc("/api/asset/{id}", dm.handleGetAsset).Methods("GET")
	r.HandleFunc("/api/asset/{id}/columns", dm.handleGetAssetColumns).Methods("GET")
	r.HandleFunc("/api/asset/{id}/select", dm.handleGetAssetSelect).Methods("GET")
	
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
