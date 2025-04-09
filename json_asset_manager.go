package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"compress/gzip"
)

// JSONAssetManager manages the JSON files for BB_ASSETS
// It implements the same interface as DataDictionary for compatibility
type JSONAssetManager struct {
	sync.RWMutex
	logger         *Logger
	jsonDir        string   // Directory for JSON files
	columns        []string // List of all columns
	idPrefixFilter []string // Optional ID_BB_GLOBAL prefix filter
	// For compatibility with DataDictionary interface
	Data map[string]map[string]string // This will be empty, just for interface compatibility
}

// NewJSONAssetManager creates a new JSON asset manager
func NewJSONAssetManager(logger *Logger, dataDir string) (*JSONAssetManager, error) {
	// Create the JSON directory if it doesn't exist
	jsonDir := filepath.Join(dataDir, "json")
	if err := os.MkdirAll(jsonDir, 0755); err != nil {
		return nil, fmt.Errorf("error creating JSON directory: %v", err)
	}

	return &JSONAssetManager{
		logger:  logger,
		jsonDir: jsonDir,
		columns: []string{},
		Data:    make(map[string]map[string]string), // Empty map for interface compatibility
	}, nil
}

// SetIDPrefixFilter sets the ID_BB_GLOBAL prefix filter
func (j *JSONAssetManager) SetIDPrefixFilter(prefixes []string) {
	j.Lock()
	defer j.Unlock()
	j.idPrefixFilter = prefixes
}

// SetIDPrefixWhitelist is an alias for SetIDPrefixFilter for compatibility with DataDictionary
func (j *JSONAssetManager) SetIDPrefixWhitelist(prefixes []string) {
	j.SetIDPrefixFilter(prefixes)
}

// ShouldIncludeID checks if an ID_BB_GLOBAL should be included based on the filter
func (j *JSONAssetManager) ShouldIncludeID(id string) bool {
	j.RLock()
	defer j.RUnlock()

	// If no filter is set, include all IDs
	if len(j.idPrefixFilter) == 0 {
		return true
	}

	// Check if the ID matches any of the filter patterns
	for _, pattern := range j.idPrefixFilter {
		// Try to compile as regex first
		regex, err := regexp.Compile(pattern)
		if err == nil {
			// It's a valid regex pattern
			if regex.MatchString(id) {
				return true
			}
		} else {
			// Fallback to simple prefix check
			if strings.HasPrefix(id, pattern) {
				return true
			}
		}
	}

	return false
}

// GetJSONFilePath returns the path to the JSON file for an ID_BB_GLOBAL
func (j *JSONAssetManager) GetJSONFilePath(id string) string {
	// Convert ID to lowercase for consistent path generation
	idLower := strings.ToLower(id)
	
	// Create the trie directory structure
	var pathParts []string
	
	// Add each character as a directory level, up to 5 levels or the length of the ID
	depth := 5
	if len(idLower) < depth {
		depth = len(idLower)
	}
	
	for i := 0; i < depth; i++ {
		pathParts = append(pathParts, string(idLower[i]))
	}
	
	// Create the directory path
	dirPath := filepath.Join(j.jsonDir, filepath.Join(pathParts...))
	
	// Ensure the directory exists
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		j.logger.Error("Error creating directory for ID %s: %v", id, err)
		return ""
	}
	
	// Return the full path to the JSON file
	return filepath.Join(dirPath, id+".json")
}

// LoadOrCreateAsset loads an asset from its JSON file or creates a new one
func (j *JSONAssetManager) LoadOrCreateAsset(id string) (map[string]string, error) {
	j.Lock()
	defer j.Unlock()
	
	filePath := j.GetJSONFilePath(id)
	if filePath == "" {
		return nil, fmt.Errorf("error getting JSON file path for ID %s", id)
	}
	
	// Check if the file exists
	asset := make(map[string]string)
	
	if _, err := os.Stat(filePath); err == nil {
		// File exists, load it
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("error reading JSON file for ID %s: %v", id, err)
		}
		
		if err := json.Unmarshal(data, &asset); err != nil {
			return nil, fmt.Errorf("error parsing JSON file for ID %s: %v", id, err)
		}
	}
	
	// Always add the ID_BB_GLOBAL field
	asset["ID_BB_GLOBAL"] = id
	
	return asset, nil
}

// SaveAsset saves an asset to its JSON file
func (j *JSONAssetManager) SaveAsset(id string, asset map[string]string) error {
	j.Lock()
	defer j.Unlock()
	
	filePath := j.GetJSONFilePath(id)
	if filePath == "" {
		return fmt.Errorf("error getting JSON file path for ID %s", id)
	}
	
	// Convert to JSON
	data, err := json.MarshalIndent(asset, "", "  ")
	if err != nil {
		return fmt.Errorf("error converting asset to JSON for ID %s: %v", id, err)
	}
	
	// Write to file
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("error writing JSON file for ID %s: %v", id, err)
	}
	
	return nil
}

// UpdateAssetFromCSV updates an asset with data from a CSV record
func (j *JSONAssetManager) UpdateAssetFromCSV(id string, header []string, record []string) error {
	// Check if the ID should be included based on the prefix filter
	if !j.ShouldIncludeID(id) {
		return nil
	}
	
	// Load or create the asset
	asset, err := j.LoadOrCreateAsset(id)
	if err != nil {
		return fmt.Errorf("error loading asset for ID %s: %v", id, err)
	}
	
	// Update the asset with the new data
	for i, value := range record {
		if i < len(header) {
			colName := header[i]
			
			// Skip empty, null, or N.A. values
			if value == "" || strings.ToLower(value) == "null" || value == "N.A." {
				continue
			}
			
			// Update the value
			asset[colName] = value
			
			// Add to columns list if not already present
			j.addColumnIfNotExists(colName)
		}
	}
	
	// For compatibility with the existing code, we'll also update the Data map
	// This is inefficient but ensures compatibility during the transition
	j.Lock()
	j.Data[id] = asset
	j.Unlock()
	
	// Save the updated asset
	return j.SaveAsset(id, asset)
}

// addColumnIfNotExists adds a column to the list if it doesn't already exist
func (j *JSONAssetManager) addColumnIfNotExists(colName string) {
	for _, existingCol := range j.columns {
		if colName == existingCol {
			return
		}
	}
	j.columns = append(j.columns, colName)
}

// GetColumns returns the list of all columns
func (j *JSONAssetManager) GetColumns() []string {
	j.RLock()
	defer j.RUnlock()
	return j.columns
}

// LoadCSVFile loads a CSV file and updates the JSON assets
func (j *JSONAssetManager) LoadCSVFile(filePath string) error {
	j.logger.Info("Loading CSV file: %s", filePath)
	
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()
	
	var reader io.Reader = file
	
	// If the file is gzipped, use a gzip reader
	if strings.HasSuffix(strings.ToLower(filePath), ".gz") {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("error creating gzip reader: %v", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}
	
	// Create a CSV reader
	csvReader := csv.NewReader(reader)
	
	// Read the header
	header, err := csvReader.Read()
	if err != nil {
		return fmt.Errorf("error reading CSV header: %v", err)
	}
	
	// Check if the file has an ID_BB_GLOBAL column
	idIndex := -1
	for i, col := range header {
		if col == "ID_BB_GLOBAL" {
			idIndex = i
			break
		}
	}
	
	// Skip files without an ID_BB_GLOBAL column
	if idIndex == -1 {
		j.logger.Warn("Skipping file %s: No ID_BB_GLOBAL column found", filePath)
		return nil
	}
	
	// Add all columns to the columns list
	for _, col := range header {
		j.addColumnIfNotExists(col)
	}
	
	// Read and process each row
	rowCount := 0
	skippedCount := 0
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			j.logger.Warn("Error reading CSV record: %v", err)
			continue
		}
		
		// Get the ID_BB_GLOBAL value
		if idIndex >= len(record) {
			j.logger.Warn("Skipping row: ID_BB_GLOBAL column index out of range")
			continue
		}
		
		id := record[idIndex]
		if id == "" {
			skippedCount++
			continue
		}
		
		// Update the asset with the CSV data
		if err := j.UpdateAssetFromCSV(id, header, record); err != nil {
			j.logger.Warn("Error updating asset for ID %s: %v", id, err)
			skippedCount++
			continue
		}
		
		rowCount++
	}
	
	j.logger.Success("Loaded %d rows from %s (skipped %d rows)", rowCount, filepath.Base(filePath), skippedCount)
	return nil
}

// LoadFiles loads multiple CSV files and updates the JSON assets
func (j *JSONAssetManager) LoadFiles(filePaths []string) error {
	for _, filePath := range filePaths {
		if err := j.LoadCSVFile(filePath); err != nil {
			j.logger.Error("Error loading file %s: %v", filePath, err)
			// Continue with other files
		}
	}
	
	// For compatibility with the existing code, we'll update the Data map
	// with a placeholder entry. The actual data is stored in JSON files.
	j.Lock()
	j.Data["placeholder"] = map[string]string{"ID_BB_GLOBAL": "placeholder"}
	j.Unlock()
	
	j.logger.Success("Processed all files, total columns: %d", len(j.columns))
	return nil
}

// GetAsset loads an asset from its JSON file
func (j *JSONAssetManager) GetAsset(id string) (map[string]string, error) {
	j.RLock()
	defer j.RUnlock()
	
	filePath := j.GetJSONFilePath(id)
	if filePath == "" {
		return nil, fmt.Errorf("error getting JSON file path for ID %s", id)
	}
	
	// Check if the file exists
	if _, err := os.Stat(filePath); err != nil {
		return nil, fmt.Errorf("asset not found for ID %s", id)
	}
	
	// Read the file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading JSON file for ID %s: %v", id, err)
	}
	
	// Parse the JSON
	asset := make(map[string]string)
	if err := json.Unmarshal(data, &asset); err != nil {
		return nil, fmt.Errorf("error parsing JSON file for ID %s: %v", id, err)
	}
	
	return asset, nil
}

// GetAssetWithColumns loads an asset and returns only the requested columns
func (j *JSONAssetManager) GetAssetWithColumns(id string, columns []string) (map[string]string, error) {
	// Load the full asset
	asset, err := j.GetAsset(id)
	if err != nil {
		return nil, err
	}
	
	// If all columns are requested, return the full asset
	if len(columns) == 1 && columns[0] == "*" {
		return asset, nil
	}
	
	// Create a new asset with only the requested columns
	result := make(map[string]string)
	for _, col := range columns {
		if value, exists := asset[col]; exists {
			result[col] = value
		}
	}
	
	return result, nil
}

// ExecuteSQLQuery executes a SQL query against the JSON assets
func (j *JSONAssetManager) ExecuteSQLQuery(sqlQuery string) ([]map[string]string, error) {
	// Parse the SQL query
	query, err := ParseSQL(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("error parsing SQL query: %v", err)
	}
	
	// Check if the table is BB_ASSETS
	if query.FromTable != "BB_ASSETS" {
		return nil, fmt.Errorf("unknown table: %s", query.FromTable)
	}
	
	// For now, we'll need to scan all JSON files to execute the query
	// In a future enhancement, we could implement indexing for faster queries
	return j.executeSQLQueryScan(query)
}

// executeSQLQueryScan scans all JSON files to execute a SQL query
func (j *JSONAssetManager) executeSQLQueryScan(query *SQLQuery) ([]map[string]string, error) {
	var results []map[string]string
	
	// Walk through the JSON directory
	err := filepath.Walk(j.jsonDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		
		// Skip directories
		if info.IsDir() {
			return nil
		}
		
		// Skip non-JSON files
		if !strings.HasSuffix(strings.ToLower(path), ".json") {
			return nil
		}
		
		// Read the JSON file
		data, err := os.ReadFile(path)
		if err != nil {
			j.logger.Warn("Error reading JSON file %s: %v", path, err)
			return nil
		}
		
		// Parse the JSON
		asset := make(map[string]string)
		if err := json.Unmarshal(data, &asset); err != nil {
			j.logger.Warn("Error parsing JSON file %s: %v", path, err)
			return nil
		}
		
		// Apply the WHERE clause if present
		if query.HasWhere {
			whereValue, exists := asset[query.WhereColumn]
			if !exists {
				return nil
			}
			
			// Check the condition
			matches := false
			switch query.WhereOperator {
			case "=":
				matches = whereValue == query.WhereValue
			case ">":
				matches = whereValue > query.WhereValue
			case "<":
				matches = whereValue < query.WhereValue
			case ">=":
				matches = whereValue >= query.WhereValue
			case "<=":
				matches = whereValue <= query.WhereValue
			case "!=":
				matches = whereValue != query.WhereValue
			}
			
			if !matches {
				return nil
			}
		}
		
		// Include the asset in the results
		if query.SelectColumns[0] == "*" {
			// Select all columns
			results = append(results, asset)
		} else {
			// Select specific columns
			selectedAsset := make(map[string]string)
			for _, col := range query.SelectColumns {
				if value, exists := asset[col]; exists {
					selectedAsset[col] = value
				}
			}
			results = append(results, selectedAsset)
		}
		
		return nil
	})
	
	if err != nil {
		return nil, fmt.Errorf("error scanning JSON files: %v", err)
	}
	
	return results, nil
}
