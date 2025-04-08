package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
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
}

func NewDataMatrix() (*DataMatrix, error) {
	// Open an in-memory DuckDB database
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("error opening DuckDB: %v", err)
	}

	dm := &DataMatrix{
		db: db,
	}

	if err := dm.loadData(); err != nil {
		db.Close()
		return nil, err
	}

	return dm, nil
}

func (dm *DataMatrix) loadData() error {
	// Get list of CSV files
	files, err := os.ReadDir("example-data")
	if err != nil {
		return fmt.Errorf("error reading directory: %v", err)
	}

	// Create temporary views for each CSV file and collect column information
	validFiles := make([]string, 0)
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(strings.ToLower(file.Name()), ".csv") {
			filePath := filepath.Join("example-data", file.Name())
			
			// Create a temporary view for the CSV
			viewName := fmt.Sprintf("temp_%s", strings.ReplaceAll(filepath.Base(file.Name()), ".", "_"))
			_, err := dm.db.Exec(fmt.Sprintf("CREATE VIEW %s AS SELECT * FROM read_csv_auto('%s')", viewName, filePath))
			if err != nil {
				log.Printf("Error creating view for %s: %v", file.Name(), err)
				continue
			}

			// Check if ID_BB_GLOBAL exists in this file
			var hasIDColumn bool
			row := dm.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) > 0 FROM pragma_table_info('%s') WHERE name = 'ID_BB_GLOBAL'", viewName))
			err = row.Scan(&hasIDColumn)
			if err != nil {
				log.Printf("Error checking for ID_BB_GLOBAL in %s: %v", file.Name(), err)
				dm.db.Exec(fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName))
				continue
			}

			if !hasIDColumn {
				log.Printf("Skipping file %s: No ID_BB_GLOBAL column found", file.Name())
				dm.db.Exec(fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName))
				continue
			}

			validFiles = append(validFiles, viewName)
		}
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
			log.Printf("Error getting columns for %s: %v", view, err)
			continue
		}

		for rows.Next() {
			var colName string
			if err := rows.Scan(&colName); err != nil {
				rows.Close()
				log.Printf("Error scanning column name: %v", err)
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

	log.Printf("Loaded data_matrix table with %d rows and %d columns", rowCount, len(dm.columns))
	return nil
}

func (dm *DataMatrix) Close() error {
	return dm.db.Close()
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
	Columns []string `json:"columns" example:"[\"ID_BB_GLOBAL\",\"Company\",\"Revenue\"]"` // Optional, defaults to ["*"]
	Where   string   `json:"where,omitempty" example:"Revenue > 200"`                      // Optional SQL WHERE clause
	Limit   int      `json:"limit,omitempty" example:"10"`                                // Optional limit for results
	Offset  int      `json:"offset,omitempty" example:"0"`                                // Optional offset for pagination
}

// QueryResponse defines the structure for the query API response
type QueryResponse struct {
	Data  []map[string]interface{} `json:"data"`  // The query results
	Count int                      `json:"count"` // Number of results returned
	Total int64                    `json:"total"` // Total number of records in the database
}

// @Summary Query the data_matrix table
// @Description Execute a SQL query against the data_matrix table with optional filtering and pagination
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

	// Construct SQL query
	query := fmt.Sprintf("SELECT %s FROM data_matrix", strings.Join(params.Columns, ", "))
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
		log.Printf("Error getting total count: %v", err)
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
	// Check if example-data directory exists, if not create test data
	if _, err := os.Stat("example-data"); os.IsNotExist(err) {
		log.Println("Creating test data...")
		if err := createTestData(); err != nil {
			log.Fatalf("Error creating test data: %v", err)
		}
	}

	dm, err := NewDataMatrix()
	if err != nil {
		log.Fatalf("Error initializing DataMatrix: %v", err)
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
	log.Printf("Starting server on port %s", port)
	log.Printf("Swagger UI available at http://localhost:%s/swagger/index.html", port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
