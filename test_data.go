package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// createTestData creates test CSV files for development and testing
func createTestData() error {
	// Create example-data directory if it doesn't exist
	if err := os.MkdirAll("example-data", 0755); err != nil {
		return fmt.Errorf("error creating directory: %v", err)
	}

	// Create subdirectories for testing recursive loading
	if err := os.MkdirAll(filepath.Join("example-data", "financial"), 0755); err != nil {
		return fmt.Errorf("error creating subdirectory: %v", err)
	}

	if err := os.MkdirAll(filepath.Join("example-data", "technology", "us"), 0755); err != nil {
		return fmt.Errorf("error creating nested subdirectory: %v", err)
	}

	// Create test file 1 with ID_BB_GLOBAL in root directory
	file1 := `ID_BB_GLOBAL,Company,Industry,Revenue
AAPL,Apple Inc.,Technology,365.8
MSFT,Microsoft Corporation,Technology,168.1
AMZN,Amazon.com Inc.,E-Commerce,386.1
GOOGL,Alphabet Inc.,Technology,182.5
FB,Meta Platforms Inc.,Social Media,86.0
`
	if err := os.WriteFile(filepath.Join("example-data", "companies.csv"), []byte(file1), 0644); err != nil {
		return fmt.Errorf("error writing file: %v", err)
	}

	// Create test file 2 with ID_BB_GLOBAL and different columns in first level subdirectory
	file2 := `ID_BB_GLOBAL,Employees,Founded,Headquarters
AAPL,154000,1976,"Cupertino, CA"
MSFT,181000,1975,"Redmond, WA"
AMZN,1335000,1994,"Seattle, WA"
GOOGL,135000,1998,"Mountain View, CA"
FB,71970,2004,"Menlo Park, CA"
TSLA,99290,2003,"Austin, TX"
`
	if err := os.WriteFile(filepath.Join("example-data", "financial", "company_details.csv"), []byte(file2), 0644); err != nil {
		return fmt.Errorf("error writing file: %v", err)
	}

	// Create test file 3 in second level subdirectory
	file3 := `ID_BB_GLOBAL,MarketCap,PE_Ratio,DividendYield
AAPL,2850.0,32.1,0.5
MSFT,2500.0,36.2,0.8
AMZN,1750.0,60.5,0.0
GOOGL,1550.0,28.3,0.0
FB,1000.0,25.7,0.0
`
	if err := os.WriteFile(filepath.Join("example-data", "technology", "us", "market_data.csv"), []byte(file3), 0644); err != nil {
		return fmt.Errorf("error writing file: %v", err)
	}

	// Create test file 4 without ID_BB_GLOBAL (should be skipped)
	file4 := `Name,Value,Date
Test1,100,2023-01-01
Test2,200,2023-01-02
Test3,300,2023-01-03
`
	if err := os.WriteFile(filepath.Join("example-data", "invalid.csv"), []byte(file4), 0644); err != nil {
		return fmt.Errorf("error writing file: %v", err)
	}

	log.Println("Test data created successfully with files in root directory and subdirectories (up to 2 levels deep)")
	return nil
}
