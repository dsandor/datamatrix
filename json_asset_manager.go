package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
	"compress/gzip"
)

// ColumnIndex represents the effective date index for a column value
type ColumnIndex struct {
	ID           string `json:"id"`           // ID_BB_GLOBAL
	ColumnName   string `json:"column_name"`  // Column/property name
	EffectiveDate string `json:"effective_date"` // Effective date in YYYYMMDD format
	SourceFile   string `json:"source_file"`   // Source file where the column value was retrieved from
}

// AssetMetadata holds the metadata for a single asset
type AssetMetadata struct {
	ID        string        `json:"id"`        // ID_BB_GLOBAL
	Columns   []ColumnIndex `json:"columns"`   // Column metadata with effective dates
	UpdatedAt time.Time     `json:"updated_at"` // Last update timestamp
}

// JSONAssetManager manages the JSON files for BB_ASSETS
// It implements the same interface as DataDictionary for compatibility
type JSONAssetManager struct {
	sync.RWMutex
	logger         *Logger
	progress       *ProgressTracker
	jsonDir        string   // Directory for JSON files
	columns        []string // List of all columns
	idPrefixFilter []string // Optional ID_BB_GLOBAL prefix filter
	// For compatibility with DataDictionary interface
	Data map[string]map[string]string // This will be empty, just for interface compatibility
	
	// Column tracking
	columnsMutex sync.RWMutex // Mutex for columns list
	
	// Cache of asset IDs for quick lookup
	assetIDs      map[string]bool // Set of known asset IDs
	assetIDsMutex sync.RWMutex   // Mutex for asset IDs map
}

// NewJSONAssetManager creates a new JSON asset manager
func NewJSONAssetManager(logger *Logger, progress *ProgressTracker, dataDir string) (*JSONAssetManager, error) {
	// Create the JSON directory if it doesn't exist
	jsonDir := filepath.Join(dataDir, "json")
	if err := os.MkdirAll(jsonDir, 0755); err != nil {
		return nil, fmt.Errorf("error creating JSON directory: %v", err)
	}
	
	// Create the asset manager
	manager := &JSONAssetManager{
		logger:        logger,
		progress:      progress,
		jsonDir:       jsonDir,
		columns:       []string{},
		idPrefixFilter: []string{},
		Data:          make(map[string]map[string]string), // Empty map for interface compatibility
		assetIDs:      make(map[string]bool),
	}
	
	// Check for legacy index file and migrate if needed
	legacyIndexPath := filepath.Join(dataDir, "asset_index.json")
	if err := manager.migrateFromLegacyIndex(legacyIndexPath); err != nil {
		logger.Warn("Error migrating from legacy index: %v", err)
	}
	
	// Scan existing assets to build column list and asset ID cache
	if err := manager.scanExistingAssets(); err != nil {
		logger.Warn("Could not scan existing assets: %v", err)
	}
	
	return manager, nil
}

// loadIndex loads the index file if it exists
// migrateFromLegacyIndex migrates data from the old single index file to per-asset metadata files
// This is a one-time migration function that can be called if needed
func (j *JSONAssetManager) migrateFromLegacyIndex(indexFilePath string) error {
	j.logger.Info("Checking for legacy index file at %s", indexFilePath)
	
	// Check if the legacy index file exists
	if _, err := os.Stat(indexFilePath); os.IsNotExist(err) {
		j.logger.Info("No legacy index file found, skipping migration")
		return nil
	}
	
	// Read the legacy index file
	data, err := os.ReadFile(indexFilePath)
	if err != nil {
		return fmt.Errorf("error reading legacy index file: %v", err)
	}
	
	// Parse the JSON
	type AssetIndex struct {
		Entries []ColumnIndex `json:"entries"`
	}
	
	var legacyIndex AssetIndex
	if err := json.Unmarshal(data, &legacyIndex); err != nil {
		return fmt.Errorf("error parsing legacy index file: %v", err)
	}
	
	j.logger.Info("Migrating %d entries from legacy index file", len(legacyIndex.Entries))
	
	// Group entries by asset ID
	assetEntries := make(map[string][]ColumnIndex)
	for _, entry := range legacyIndex.Entries {
		assetEntries[entry.ID] = append(assetEntries[entry.ID], entry)
	}
	
	// Create metadata files for each asset
	migratedCount := 0
	for id, entries := range assetEntries {
		metadata := &AssetMetadata{
			ID:        id,
			Columns:   entries,
			UpdatedAt: time.Now(),
		}
		
		if err := j.saveAssetMetadata(id, metadata); err != nil {
			j.logger.Warn("Error saving metadata for asset %s: %v", id, err)
			continue
		}
		
		migratedCount++
	}
	
	j.logger.Success("Migrated %d assets from legacy index file", migratedCount)
	
	// Rename the legacy index file to indicate it's been migrated
	backupPath := indexFilePath + ".migrated"
	if err := os.Rename(indexFilePath, backupPath); err != nil {
		j.logger.Warn("Error renaming legacy index file: %v", err)
	}
	
	return nil
}

// getEffectiveDateFromFilename extracts the YYYYMMDD date from a filename
func (j *JSONAssetManager) getEffectiveDateFromFilename(filename string) string {
	// Extract date using regex - looking for 8 consecutive digits (YYYYMMDD)
	re := regexp.MustCompile(`\d{8}`)
	match := re.FindString(filename)
	
	if match != "" {
		// Validate the date
		if _, err := time.Parse("20060102", match); err == nil {
			return match
		}
	}
	
	// If no valid date found, use today's date as fallback
	return time.Now().Format("20060102")
}

// getColumnEffectiveDate gets the effective date for a column from the asset metadata
func (j *JSONAssetManager) getColumnEffectiveDate(id, columnName string) string {
	// Load the metadata for the asset
	metadata, err := j.loadAssetMetadata(id)
	if err != nil {
		// If there's an error, just return empty string
		return ""
	}
	
	// Look for the column in the metadata
	for _, col := range metadata.Columns {
		if col.ColumnName == columnName {
			return col.EffectiveDate
		}
	}
	
	return "" // No effective date found
}

// updateColumnEffectiveDate updates the effective date for a column in the asset metadata
func (j *JSONAssetManager) updateColumnEffectiveDate(id, columnName, effectiveDate, sourceFile string) error {
	// Load the metadata for the asset
	metadata, err := j.loadAssetMetadata(id)
	if err != nil {
		// If there's an error loading, create a new metadata file
		metadata = &AssetMetadata{
			ID:        id,
			Columns:   []ColumnIndex{},
			UpdatedAt: time.Now(),
		}
	}
	
	// Check if the column already exists
	columnExists := false
	for i, col := range metadata.Columns {
		if col.ColumnName == columnName {
			// Only update if the new date is newer
			if effectiveDate > col.EffectiveDate {
				metadata.Columns[i].EffectiveDate = effectiveDate
				metadata.Columns[i].SourceFile = sourceFile
			}
			columnExists = true
			break
		}
	}
	
	// If the column doesn't exist, add it
	if !columnExists {
		metadata.Columns = append(metadata.Columns, ColumnIndex{
			ID:            id,
			ColumnName:    columnName,
			EffectiveDate: effectiveDate,
			SourceFile:    sourceFile,
		})
	}
	
	// Save the updated metadata
	return j.saveAssetMetadata(id, metadata)
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
	
	// Use every character in the ID for the directory structure
	for i := 0; i < len(idLower); i++ {
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
// This is kept for backward compatibility
func (j *JSONAssetManager) UpdateAssetFromCSV(id string, header []string, record []string) error {
	// Use current date as effective date for backward compatibility
	_, err := j.UpdateAssetFromCSVWithDate(id, header, record, time.Now().Format("20060102"), "manual_update")
	return err
}

// UpdateAssetFromCSVWithDate updates an asset with data from a CSV record with effective date
// Returns true if any values were updated, false otherwise
func (j *JSONAssetManager) UpdateAssetFromCSVWithDate(id string, header []string, record []string, effectiveDate string, sourceFile string) (bool, error) {
	// Check if the ID should be included based on the prefix filter
	if !j.ShouldIncludeID(id) {
		return false, nil
	}
	
	// Load or create the asset
	asset, err := j.LoadOrCreateAsset(id)
	if err != nil {
		return false, fmt.Errorf("error loading asset for ID %s: %v", id, err)
	}
	
	// Track if any values were updated
	updated := false
	
	// Update the asset with the new data
	for i, value := range record {
		if i < len(header) {
			colName := header[i]
			
			// Skip empty, null, or N.A. values
			if value == "" || strings.ToLower(value) == "null" || value == "N.A." {
				continue
			}
			
			// Check if we should update this column based on effective date
			currentEffectiveDate := j.getColumnEffectiveDate(id, colName)
			
			// Update if:
			// 1. No effective date exists for this column (first time seeing it)
			// 2. The new effective date is newer than the current one
			if currentEffectiveDate == "" || effectiveDate > currentEffectiveDate {
				// Update the value
				asset[colName] = value
				
				// Update the effective date in the metadata with source file information
				if err := j.updateColumnEffectiveDate(id, colName, effectiveDate, sourceFile); err != nil {
					j.logger.Warn("Error updating column metadata for %s.%s: %v", id, colName, err)
				}
				
				updated = true
				
				// Add to columns list if not already present
				j.addColumnIfNotExists(colName)
			}
		}
	}
	
	// For compatibility with the existing code, we'll also update the Data map
	// This is inefficient but ensures compatibility during the transition
	j.Lock()
	j.Data[id] = asset
	j.Unlock()
	
	// Only save if something was updated
	if updated {
		// Save the updated asset
		if err := j.SaveAsset(id, asset); err != nil {
			return false, err
		}
	}
	
	return updated, nil
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
// This method is designed to be called from multiple goroutines in parallel
func (j *JSONAssetManager) LoadCSVFile(filePath string) error {
	fileName := filepath.Base(filePath)
	j.logger.Info("Loading CSV file: %s", filePath)
	
	// Create a local progress tracker for this file to avoid lock contention
	// when multiple goroutines are processing files simultaneously
	fileProgress := NewProgressTracker(j.logger)
	fileProgress.StartProgress(fmt.Sprintf("Loading %s", fileName), 0)
	
	// Extract the effective date from the filename
	effectiveDate := j.getEffectiveDateFromFilename(filePath)
	j.logger.Info("Effective date for file %s: %s", fileName, effectiveDate)
	
	// Update progress to show we're opening the file
	fileProgress.SetStatus(fmt.Sprintf("Opening file %s", fileName))
	
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
	
	// Update progress to show we're reading the CSV header
	fileProgress.SetStatus(fmt.Sprintf("Reading header from %s", fileName))
	
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
	updatedCount := 0
	
	// Update progress status
	fileProgress.SetStatus(fmt.Sprintf("Enumerating rows in %s", fileName))
	
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
		
		// Update progress with current row count
		rowCount++
		if rowCount % 10 == 0 { // Update every 10 rows to keep the progress tracker active
			fileProgress.UpdateProgress(rowCount, fmt.Sprintf("Enumerating %s: %d rows", fileName, rowCount))
		}
		
		// Update the asset with the CSV data and track if updates were made
		updated, err := j.UpdateAssetFromCSVWithDate(id, header, record, effectiveDate, filePath)
		if err != nil {
			j.logger.Warn("Error updating asset for ID %s: %v", id, err)
			skippedCount++
			continue
		}
		
		if updated {
			updatedCount++
		}
	}
	
	// Update progress to show we're done processing the file
	fileProgress.SetStatus(fmt.Sprintf("Completed processing %s", fileName))
	
	// Complete progress tracking
	fileProgress.CompleteProgress(fmt.Sprintf("Completed processing %s", fileName))
	
	j.logger.Success("Loaded %d rows from %s (updated %d, skipped %d rows)", 
		rowCount, filepath.Base(filePath), updatedCount, skippedCount)
	return nil
}

// LoadFiles loads multiple CSV files and updates the JSON assets
func (j *JSONAssetManager) LoadFiles(filePaths []string) error {
	// Start progress tracking for overall file loading
	j.progress.StartProgress("Loading CSV files", len(filePaths))
	j.logger.Info("Starting to process %d CSV files", len(filePaths))
	
	if len(filePaths) == 0 {
		j.logger.Info("No CSV files to process")
		return nil
	}
	
	// Create a worker pool for parallel processing
	numWorkers := runtime.NumCPU() // Use number of CPU cores for worker count
	if numWorkers > 8 {
		numWorkers = 8 // Cap at 8 workers to avoid excessive resource usage
	}
	if numWorkers > len(filePaths) {
		numWorkers = len(filePaths) // Don't create more workers than files
	}
	
	j.logger.Info("Using %d worker goroutines for parallel file processing", numWorkers)
	
	// Create a channel for work items
	jobs := make(chan string, len(filePaths))
	
	// Create a wait group to wait for all workers to finish
	var wg sync.WaitGroup
	
	// Create a mutex for updating progress safely
	progressMutex := &sync.Mutex{}
	processedCount := 0
	
	// Create worker goroutines
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			
			for filePath := range jobs {
				fileName := filepath.Base(filePath)
				j.logger.Info("Worker %d processing file: %s", workerID, fileName)
				
				// Process the file
				if err := j.LoadCSVFile(filePath); err != nil {
					j.logger.Error("Error loading file %s: %v", filePath, err)
					// Continue with other files
				}
				
				// Update progress safely
				progressMutex.Lock()
				processedCount++
				j.progress.UpdateProgress(processedCount, fmt.Sprintf("Processed %d of %d files", processedCount, len(filePaths)))
				progressMutex.Unlock()
			}
		}(w)
	}
	
	// Send all files to the job channel
	for _, filePath := range filePaths {
		jobs <- filePath
	}
	
	// Close the job channel to signal no more work
	close(jobs)
	
	// Wait for all workers to finish
	wg.Wait()
	
	// For compatibility with the existing code, we'll update the Data map
	// with a placeholder entry. The actual data is stored in JSON files.
	j.Lock()
	j.Data["placeholder"] = map[string]string{"ID_BB_GLOBAL": "placeholder"}
	j.Unlock()
	
	// Update progress to show we're finalizing processing
	j.progress.SetStatus("Finalizing asset processing")
	
	// No need to save a central index anymore as we use per-asset metadata files
	
	// Complete overall progress tracking
	j.progress.CompleteProgress("All CSV files processed successfully")
	
	// Set system to idle state
	j.progress.SetStatus("Idle - Ready for queries")
	
	j.assetIDsMutex.RLock()
	assetCount := len(j.assetIDs)
	j.assetIDsMutex.RUnlock()
	
	j.logger.Success("Processed all files, total columns: %d, total assets: %d", 
		len(j.columns), assetCount)
	return nil
}

// scanExistingAssets scans the JSON directory for existing assets and builds the column list and asset ID cache
func (j *JSONAssetManager) scanExistingAssets() error {
	j.logger.Info("Scanning existing assets in %s", j.jsonDir)
	
	// Start progress tracking
	j.progress.StartProgress("Scanning existing assets", 0)
	
	// Use a map to track unique columns
	colMap := make(map[string]bool)
	assetCount := 0
	
	// Walk the JSON directory recursively
	err := filepath.Walk(j.jsonDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		
		// Skip directories
		if info.IsDir() {
			return nil
		}
		
		// Only process JSON files
		if !strings.HasSuffix(path, ".json") {
			return nil
		}
		
		// Skip metadata files
		if strings.HasSuffix(path, ".metadata.json") {
			return nil
		}
		
		// Extract the ID from the filename
		id := strings.TrimSuffix(filepath.Base(path), ".json")
		
		// Add to asset IDs cache
		j.assetIDsMutex.Lock()
		j.assetIDs[id] = true
		j.assetIDsMutex.Unlock()
		
		// Load the metadata file
		metadata, err := j.loadAssetMetadata(id)
		if err != nil {
			// If metadata doesn't exist, just skip
			return nil
		}
		
		// Add columns to the column map
		for _, col := range metadata.Columns {
			colMap[col.ColumnName] = true
		}
		
		assetCount++
		if assetCount % 100 == 0 {
			j.progress.UpdateProgress(assetCount, fmt.Sprintf("Scanned %d assets", assetCount))
		}
		
		return nil
	})
	
	// Convert the column map to a slice
	j.columnsMutex.Lock()
	for col := range colMap {
		j.columns = append(j.columns, col)
	}
	j.columnsMutex.Unlock()
	
	j.progress.CompleteProgress(fmt.Sprintf("Scanned %d assets with %d unique columns", assetCount, len(colMap)))
	j.logger.Info("Scanned %d assets with %d unique columns", assetCount, len(colMap))
	
	return err
}

// getAssetMetadataPath returns the path to the metadata file for an asset
func (j *JSONAssetManager) getAssetMetadataPath(id string) string {
	// Get the directory path for the asset
	dirPath := filepath.Dir(j.GetJSONFilePath(id))
	
	// Return the path to the metadata file
	return filepath.Join(dirPath, id+".metadata.json")
}

// loadAssetMetadata loads the metadata for an asset
func (j *JSONAssetManager) loadAssetMetadata(id string) (*AssetMetadata, error) {
	metadataPath := j.getAssetMetadataPath(id)
	
	// Check if the file exists
	if _, err := os.Stat(metadataPath); err != nil {
		// Create a new metadata file if it doesn't exist
		metadata := &AssetMetadata{
			ID:        id,
			Columns:   []ColumnIndex{},
			UpdatedAt: time.Now(),
		}
		
		// Save the new metadata file
		if err := j.saveAssetMetadata(id, metadata); err != nil {
			return nil, err
		}
		
		return metadata, nil
	}
	
	// Read the file
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, fmt.Errorf("error reading metadata file for ID %s: %v", id, err)
	}
	
	// Parse the JSON
	metadata := &AssetMetadata{}
	if err := json.Unmarshal(data, metadata); err != nil {
		return nil, fmt.Errorf("error parsing metadata file for ID %s: %v", id, err)
	}
	
	return metadata, nil
}

// saveAssetMetadata saves the metadata for an asset
func (j *JSONAssetManager) saveAssetMetadata(id string, metadata *AssetMetadata) error {
	metadataPath := j.getAssetMetadataPath(id)
	
	// Update the timestamp
	metadata.UpdatedAt = time.Now()
	
	// Convert to JSON
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("error converting metadata to JSON for ID %s: %v", id, err)
	}
	
	// Ensure the directory exists
	dirPath := filepath.Dir(metadataPath)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("error creating directory for ID %s: %v", id, err)
	}
	
	// Write to file
	if err := os.WriteFile(metadataPath, data, 0644); err != nil {
		return fmt.Errorf("error writing metadata file for ID %s: %v", id, err)
	}
	
	return nil
}

// GetIndexInfo returns information about the assets and columns
func (j *JSONAssetManager) GetIndexInfo() map[string]interface{} {
	j.assetIDsMutex.RLock()
	assetCount := len(j.assetIDs)
	j.assetIDsMutex.RUnlock()
	
	j.columnsMutex.RLock()
	columnCount := len(j.columns)
	j.columnsMutex.RUnlock()
	
	return map[string]interface{}{
		"asset_count":    assetCount,
		"column_count":   columnCount,
		"storage_type":   "distributed",
		"storage_path":   j.jsonDir,
	}
}

// GetAssetColumnMetadata returns all column metadata for a specific asset
func (j *JSONAssetManager) GetAssetColumnMetadata(id string) (map[string]map[string]string, error) {
	// Load the metadata for the asset
	metadata, err := j.loadAssetMetadata(id)
	if err != nil {
		return nil, fmt.Errorf("error loading metadata for asset %s: %v", id, err)
	}
	
	// Create a map of column name to metadata
	result := make(map[string]map[string]string)
	
	// Add each column's metadata
	for _, col := range metadata.Columns {
		result[col.ColumnName] = map[string]string{
			"effective_date": col.EffectiveDate,
			"source_file":    col.SourceFile,
		}
	}
	
	return result, nil
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
