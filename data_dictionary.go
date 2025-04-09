package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"compress/gzip"
	"path/filepath"
)

// DataDictionary represents the in-memory data structure for BB_ASSETS
type DataDictionary struct {
	// Map of ID_BB_GLOBAL to a map of column name to value
	Data map[string]map[string]string
	// List of all column names
	Columns []string
	// Logger for output
	logger *Logger
	// ID_BB_GLOBAL prefix whitelist
	IDPrefixWhitelist []string
}

// NewDataDictionary creates a new data dictionary
func NewDataDictionary(logger *Logger) *DataDictionary {
	return &DataDictionary{
		Data:    make(map[string]map[string]string),
		Columns: []string{},
		logger:  logger,
	}
}

// SetIDPrefixWhitelist sets the ID_BB_GLOBAL prefix whitelist
func (d *DataDictionary) SetIDPrefixWhitelist(prefixes []string) {
	d.IDPrefixWhitelist = prefixes
}

// ShouldIncludeID checks if an ID_BB_GLOBAL should be included based on the whitelist
func (d *DataDictionary) ShouldIncludeID(id string) bool {
	// If no whitelist is set, include all IDs
	if len(d.IDPrefixWhitelist) == 0 {
		return true
	}
	
	// Check if the ID matches any of the whitelist patterns
	for _, pattern := range d.IDPrefixWhitelist {
		// Try to compile as regex first
		regex, err := regexp.Compile(pattern)
		if err == nil {
			// It's a valid regex pattern
			if regex.MatchString(id) {
				return true
			}
		} else {
			// Fallback to simple prefix check for backward compatibility
			if strings.HasPrefix(id, pattern) {
				return true
			}
		}
	}
	
	return false
}

// LoadCSVFile loads a CSV file into the data dictionary
func (d *DataDictionary) LoadCSVFile(filePath string) error {
	d.logger.Info("Loading CSV file: %s", filePath)
	
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
		d.logger.Warn("Skipping file %s: No ID_BB_GLOBAL column found", filePath)
		return nil
	}
	
	// Add new columns to the list
	for _, col := range header {
		found := false
		for _, existingCol := range d.Columns {
			if col == existingCol {
				found = true
				break
			}
		}
		if !found {
			d.Columns = append(d.Columns, col)
		}
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
			d.logger.Warn("Error reading CSV record: %v", err)
			continue
		}
		
		// Get the ID_BB_GLOBAL value
		if idIndex >= len(record) {
			d.logger.Warn("Skipping row: ID_BB_GLOBAL column index out of range")
			continue
		}
		
		id := record[idIndex]
		if id == "" {
			skippedCount++
			continue
		}
		
		// Check if the ID should be included based on the prefix whitelist
		if !d.ShouldIncludeID(id) {
			skippedCount++
			continue
		}
		
		// Create or get the record
		idRecord, exists := d.Data[id]
		if !exists {
			idRecord = make(map[string]string)
			d.Data[id] = idRecord
		}
		
		// Add each column value to the record
		for i, value := range record {
			if i < len(header) {
				colName := header[i]
				// Only add the column if it doesn't already exist
				if _, exists := idRecord[colName]; !exists {
					idRecord[colName] = value
				}
			}
		}
		
		rowCount++
	}
	
	d.logger.Success("Loaded %d rows from %s (skipped %d rows)", rowCount, filepath.Base(filePath), skippedCount)
	return nil
}

// LoadFiles loads multiple CSV files into the data dictionary
func (d *DataDictionary) LoadFiles(filePaths []string) error {
	for _, filePath := range filePaths {
		if err := d.LoadCSVFile(filePath); err != nil {
			d.logger.Error("Error loading file %s: %v", filePath, err)
			// Continue with other files
		}
	}
	
	d.logger.Success("Loaded %d unique ID_BB_GLOBAL records with %d columns", len(d.Data), len(d.Columns))
	return nil
}

// ExecuteSQLQuery executes a SQL query against the data dictionary
func (d *DataDictionary) ExecuteSQLQuery(sqlQuery string) ([]map[string]string, error) {
	// Parse the SQL query
	query, err := ParseSQL(sqlQuery)
	if err != nil {
		return nil, fmt.Errorf("error parsing SQL query: %v", err)
	}
	
	// Check if the table is BB_ASSETS
	if query.FromTable != "BB_ASSETS" {
		return nil, fmt.Errorf("unknown table: %s", query.FromTable)
	}
	
	// Execute the query
	return ExecuteQuery(query, d.Data)
}
