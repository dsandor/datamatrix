package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
	"bytes"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3File represents a file in an S3 bucket
type S3File struct {
	Key          string
	LastModified time.Time
	Size         int64
	Directory    string // The directory path within the bucket
}

// S3Loader handles loading data from S3
type S3Loader struct {
	client          *s3.Client
	logger          *Logger
	progress        *ProgressTracker
	dataDir         string   // Local directory to store downloaded files
	prefix          string   // Optional prefix within the bucket
	dirWhitelist    []string // Optional whitelist of directory names
	idPrefixFilter  []string // Optional ID_BB_GLOBAL prefix filter
}

// NewS3Loader creates a new S3Loader instance
func NewS3Loader(logger *Logger, progress *ProgressTracker, dataDir string, prefix string, dirWhitelist []string, idPrefixFilter []string) (*S3Loader, error) {
	// Create the data directory if it doesn't exist
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("error creating data directory: %v", err)
	}

	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("error loading AWS config: %v", err)
	}

	// Create S3 client
	client := s3.NewFromConfig(cfg)

	return &S3Loader{
		client:         client,
		logger:         logger,
		progress:       progress,
		dataDir:        dataDir,
		prefix:         prefix,
		dirWhitelist:   dirWhitelist,
		idPrefixFilter: idPrefixFilter,
	}, nil
}

// ListBucketContents lists all objects in the specified bucket with optional prefix
func (s *S3Loader) ListBucketContents(bucketName string) ([]S3File, error) {
	if s.prefix != "" {
		s.logger.Info("Listing contents of S3 bucket: %s with prefix: %s", bucketName, s.prefix)
	} else {
		s.logger.Info("Listing contents of S3 bucket: %s", bucketName)
	}

	// Start progress tracking
	s.progress.StartProgress("Listing S3 files", 0)

	var files []S3File
	var continuationToken *string
	pageCount := 0

	// S3 returns paginated results, so we need to loop until we've got everything
	for {
		// Prepare the input parameters for each iteration
		params := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucketName),
			ContinuationToken: continuationToken,
		}
		
		// Add prefix if specified
		if s.prefix != "" {
			params.Prefix = aws.String(s.prefix)
		}

		// Make the API call
		resp, err := s.client.ListObjectsV2(context.TODO(), params)
		if err != nil {
			return nil, fmt.Errorf("error listing S3 objects: %v", err)
		}
		
		// Update progress
		pageCount++
		s.progress.UpdateProgress(pageCount, fmt.Sprintf("Listing S3 files (page %d)", pageCount))

		// Process the objects in this page
		for _, obj := range resp.Contents {
			key := *obj.Key
			
			// Skip directories (objects ending with /)
			if strings.HasSuffix(key, "/") == true {
				continue
			}
			
			// Include CSV files (plain or gzipped) and any potentially gzipped files
			// We'll be more inclusive here and filter out non-CSV content when downloading
			lowerKey := strings.ToLower(key)
			if !strings.HasSuffix(lowerKey, ".csv") && 
			   !strings.HasSuffix(lowerKey, ".csv.gz") && 
			   !strings.HasSuffix(lowerKey, ".gz") && 
			   !strings.Contains(lowerKey, "csv") {
				continue
			}

			// Extract directory path
			dir := filepath.Dir(key)
			if dir == "." {
				dir = "" // Root directory
			}

			files = append(files, S3File{
				Key:          key,
				LastModified: *obj.LastModified,
				Size:         *obj.Size,
				Directory:    dir,
			})
		}

		// If there are more results, continue
		if resp.IsTruncated != nil && *resp.IsTruncated {
			continuationToken = resp.NextContinuationToken
		} else {
			break
		}
	}

	// Complete progress
	s.progress.CompleteProgress()
	s.logger.Success("Found %d CSV files in bucket %s", len(files), bucketName)
	return files, nil
}

// GroupFilesByDirectory groups files by their directory path
// If a directory whitelist is provided, only directories containing any of the whitelist terms will be included
func (s *S3Loader) GroupFilesByDirectory(files []S3File) map[string][]S3File {
	s.logger.Info("Grouping files by directory")
	
	// Start progress tracking
	s.progress.StartProgress("Grouping files by directory", len(files))
	
	// Group files by directory
	dirMap := make(map[string][]S3File)
	for i, file := range files {
		// Update progress
		s.progress.UpdateProgress(i+1, "")
		// Check if we should include this directory based on the whitelist
		if len(s.dirWhitelist) > 0 {
			includeDir := false
			for _, pattern := range s.dirWhitelist {
				// Try to compile as regex first
				regex, err := regexp.Compile(pattern)
				if err == nil {
					// It's a valid regex pattern
					if regex.MatchString(file.Directory) {
						includeDir = true
						break
					}
				} else {
					// Fallback to simple string contains for backward compatibility
					if strings.Contains(strings.ToLower(file.Directory), strings.ToLower(pattern)) {
						includeDir = true
						break
					}
				}
			}
			if !includeDir {
				continue
			}
		}
		
		dirMap[file.Directory] = append(dirMap[file.Directory], file)
	}

	// Sort files in each directory by LastModified (newest first)
	for dir, dirFiles := range dirMap {
		sort.Slice(dirFiles, func(i, j int) bool {
			return dirFiles[i].LastModified.After(dirFiles[j].LastModified)
		})
		dirMap[dir] = dirFiles
		s.logger.Debug("Directory %s has %d files, newest is %s (%s)", 
			dir, len(dirFiles), filepath.Base(dirFiles[0].Key), dirFiles[0].LastModified.Format(time.RFC3339))
	}

	// Complete progress
	s.progress.CompleteProgress()
	s.logger.Success("Grouped files into %d directories", len(dirMap))
	return dirMap
}

// DownloadNewestFiles downloads the newest file from each directory
// If the file already exists locally and has the same or newer timestamp, it won't be re-downloaded
func (s *S3Loader) DownloadNewestFiles(bucketName string, dirMap map[string][]S3File) ([]string, error) {
	s.logger.Info("Checking for newest files from each directory")
	
	// Count total files to download (one per directory)
	totalDirs := len(dirMap)
	s.progress.StartProgress("Downloading files", totalDirs)
	
	downloader := manager.NewDownloader(s.client)
	var downloadedFiles []string

	for _, files := range dirMap {
		if len(files) == 0 {
			continue
		}

		// Get the newest file (already sorted)
		newestFile := files[0]
		
		// Preserve the original directory structure
		localDir := filepath.Dir(filepath.Join(s.dataDir, newestFile.Key))
		if err := os.MkdirAll(localDir, 0755); err != nil {
			s.logger.Error("Error creating local directory %s: %v", localDir, err)
			continue
		}

		// Create local file path with the exact same structure as in S3
		localFilePath := filepath.Join(s.dataDir, newestFile.Key)
		
		// Check if the file already exists locally
		fileInfo, err := os.Stat(localFilePath)
		if err == nil {
			// File exists, check if it's newer or same age as the S3 file
			localModTime := fileInfo.ModTime()
			
			// If local file is newer or same age, skip download
			if !localModTime.Before(newestFile.LastModified) {
				s.logger.Info("Skipping download of %s - local file is up to date (local: %s, remote: %s)", 
					newestFile.Key, 
					localModTime.Format(time.RFC3339),
					newestFile.LastModified.Format(time.RFC3339))
				
				// Verify the file is a valid CSV or gzipped CSV
				if isValidDataFile(localFilePath) {
					downloadedFiles = append(downloadedFiles, localFilePath)
					continue
				} else {
					s.logger.Warn("Local file %s is not valid, will re-download", localFilePath)
					// Continue to download as the local file is invalid
				}
			} else {
				s.logger.Info("Local file %s is older than S3 version, will re-download", newestFile.Key)
			}
		}

		// Create the file
		s.logger.Debug("Downloading %s to %s", newestFile.Key, localFilePath)
		file, err := os.Create(localFilePath)
		if err != nil {
			s.logger.Error("Error creating local file %s: %v", localFilePath, err)
			continue
		}

		// Create a custom S3 client with logging disabled for this operation
		clientOptions := func(o *s3.Options) {
			// Disable logging for this client to suppress checksum warnings
			o.Logger = nil
		}

		// Set client options to suppress checksum warnings
		downloadOptions := func(d *manager.Downloader) {
			// Add the client options to suppress warnings
			d.ClientOptions = append(d.ClientOptions, clientOptions)
		}

		// Download the file with modified options
		_, err = downloader.Download(context.TODO(), file, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(newestFile.Key),
		}, downloadOptions)
		file.Close()

		if err != nil {
			s.logger.Error("Error downloading file %s: %v", newestFile.Key, err)
			os.Remove(localFilePath) // Clean up partial download
			continue
		}
		
		// Verify the file is a valid CSV or gzipped CSV
		if !isValidDataFile(localFilePath) {
			s.logger.Warn("Skipping file %s: Not a valid CSV or gzipped CSV file", newestFile.Key)
			os.Remove(localFilePath) // Clean up invalid file
			continue
		}

		// Set the file modification time to match the S3 file's LastModified time
		if err := os.Chtimes(localFilePath, newestFile.LastModified, newestFile.LastModified); err != nil {
			s.logger.Warn("Failed to set modification time for %s: %v", localFilePath, err)
		}

		s.logger.Success("Downloaded %s (%.2f MB, modified %s)", 
			newestFile.Key, 
			float64(newestFile.Size)/(1024*1024),
			newestFile.LastModified.Format(time.RFC3339))
		
		downloadedFiles = append(downloadedFiles, localFilePath)
	}

	s.logger.Success("Downloaded %d files from S3 bucket %s", len(downloadedFiles), bucketName)
	return downloadedFiles, nil
}

// LoadFromS3 loads data from an S3 bucket, finding the newest file in each directory
// and downloading it to the local data directory
func (s *S3Loader) LoadFromS3(bucketName string) ([]string, error) {
	// List all files in the bucket
	files, err := s.ListBucketContents(bucketName)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no CSV files found in bucket %s", bucketName)
	}

	// Group files by directory
	dirMap := s.GroupFilesByDirectory(files)

	// Download the newest file from each directory
	return s.DownloadNewestFiles(bucketName, dirMap)
}

// isValidDataFile checks if a file is a valid CSV or gzipped CSV file
func isValidDataFile(filePath string) bool {
	// Check file extension first - accept any .csv or .gz file
	lowerPath := strings.ToLower(filePath)
	if strings.HasSuffix(lowerPath, ".csv") || strings.HasSuffix(lowerPath, ".gz") || strings.HasSuffix(lowerPath, ".csv.gz") {
		return true
	}
	
	// For files without the expected extension, do a more thorough check
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return false
	}
	defer file.Close()

	// Read the first few bytes to check for gzip magic number
	buf := make([]byte, 512)
	n, err := file.Read(buf)
	if err != nil && err != io.EOF {
		return false
	}
	buf = buf[:n]

	// Check if it's a gzip file (magic number: 0x1F 0x8B)
	isGzip := bytes.HasPrefix(buf, []byte{0x1F, 0x8B})

	// If it's a gzip file, we'll assume it's a valid gzipped CSV
	if isGzip {
		return true
	}

	// For non-gzip files, do a more permissive check for CSV-like content
	// Just check if the file has some text content
	isText := true
	for _, b := range buf {
		// Check for non-printable, non-whitespace characters
		if b < 32 && b != '\t' && b != '\n' && b != '\r' {
			// Allow a few binary characters before deciding it's not text
			isText = false
			break
		}
	}

	// If it looks like text, assume it's a valid data file
	return isText
}

// CleanupDataDirectory removes all files from the data directory
func (s *S3Loader) CleanupDataDirectory() error {
	s.logger.Info("Cleaning up data directory: %s", s.dataDir)
	
	// Check if directory exists
	if _, err := os.Stat(s.dataDir); os.IsNotExist(err) {
		return nil // Directory doesn't exist, nothing to clean
	}

	// Read directory
	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		return fmt.Errorf("error reading data directory: %v", err)
	}

	// Remove each entry
	for _, entry := range entries {
		path := filepath.Join(s.dataDir, entry.Name())
		
		if entry.IsDir() {
			// Recursively remove directory
			if err := os.RemoveAll(path); err != nil {
				s.logger.Error("Error removing directory %s: %v", path, err)
			}
		} else {
			// Remove file
			if err := os.Remove(path); err != nil {
				s.logger.Error("Error removing file %s: %v", path, err)
			}
		}
	}

	s.logger.Success("Cleaned up data directory")
	return nil
}

// CopyS3FilesToLocal copies files from S3 to a local directory
func CopyS3FilesToLocal(logger *Logger, progress *ProgressTracker, bucketName, prefix, dataDir string, dirWhitelist []string, idPrefixFilter []string) ([]string, error) {
	if prefix != "" {
		logger.Info("Loading data from S3 bucket: %s with prefix: %s", bucketName, prefix)
	} else {
		logger.Info("Loading data from S3 bucket: %s", bucketName)
	}
	
	// Create S3 loader
	s3Loader, err := NewS3Loader(logger, progress, dataDir, prefix, dirWhitelist, idPrefixFilter)
	if err != nil {
		return nil, fmt.Errorf("error creating S3 loader: %v", err)
	}
	
	// Log whitelist and filter settings
	if len(dirWhitelist) > 0 {
		logger.Info("Using directory whitelist: %v", dirWhitelist)
	}
	if len(idPrefixFilter) > 0 {
		logger.Info("Using ID_BB_GLOBAL prefix filter: %v", idPrefixFilter)
	}

	// No longer cleaning up data directory before downloading to preserve existing files

	// Load data from S3
	downloadedFiles, err := s3Loader.LoadFromS3(bucketName)
	if err != nil {
		return nil, fmt.Errorf("error loading data from S3: %v", err)
	}

	logger.Memory("Memory usage after S3 download: %s", GetMemoryUsageSummary())
	return downloadedFiles, nil
}
