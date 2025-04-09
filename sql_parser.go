package main

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// SQLQuery represents a parsed SQL query
type SQLQuery struct {
	SelectColumns []string
	FromTable     string
	WhereColumn   string
	WhereOperator string
	WhereValue    string
	HasWhere      bool
}

// ParseSQL parses a simple SQL query and returns a SQLQuery struct
func ParseSQL(query string) (*SQLQuery, error) {
	// Normalize the query
	query = strings.TrimSpace(query)
	query = regexp.MustCompile(`\s+`).ReplaceAllString(query, " ")
	
	// Basic validation
	if !strings.HasPrefix(strings.ToUpper(query), "SELECT ") {
		return nil, errors.New("query must start with SELECT")
	}
	
	// Initialize the result
	result := &SQLQuery{
		HasWhere: false,
	}
	
	// Extract the FROM part
	fromParts := strings.Split(strings.ToUpper(query), " FROM ")
	if len(fromParts) != 2 {
		return nil, errors.New("query must contain FROM clause")
	}
	
	// Extract the SELECT columns
	selectPart := strings.TrimPrefix(fromParts[0], "SELECT ")
	selectColumns := strings.Split(selectPart, ",")
	for i, col := range selectColumns {
		selectColumns[i] = strings.TrimSpace(col)
	}
	result.SelectColumns = selectColumns
	
	// Extract the table and WHERE clause
	tableAndWhere := fromParts[1]
	whereParts := strings.Split(tableAndWhere, " WHERE ")
	
	if len(whereParts) > 2 {
		return nil, errors.New("query contains multiple WHERE clauses")
	}
	
	result.FromTable = strings.TrimSpace(whereParts[0])
	
	// Process WHERE clause if it exists
	if len(whereParts) == 2 {
		result.HasWhere = true
		whereClause := whereParts[1]
		
		// Parse the WHERE condition (only support simple equality for now)
		// Look for =, >, <, >=, <=, != operators
		operatorRegex := regexp.MustCompile(`\s*(=|>|<|>=|<=|!=)\s*`)
		operatorMatches := operatorRegex.FindStringSubmatch(whereClause)
		
		if len(operatorMatches) < 2 {
			return nil, errors.New("WHERE clause must contain a valid operator (=, >, <, >=, <=, !=)")
		}
		
		operator := operatorMatches[1]
		whereParts := operatorRegex.Split(whereClause, 2)
		
		if len(whereParts) != 2 {
			return nil, errors.New("invalid WHERE clause format")
		}
		
		result.WhereColumn = strings.TrimSpace(whereParts[0])
		result.WhereOperator = operator
		
		// Handle string literals in WHERE clause
		whereValue := strings.TrimSpace(whereParts[1])
		if strings.HasPrefix(whereValue, "'") && strings.HasSuffix(whereValue, "'") {
			// String literal
			result.WhereValue = whereValue[1 : len(whereValue)-1]
		} else {
			// Numeric or other value
			result.WhereValue = whereValue
		}
	}
	
	return result, nil
}

// ExecuteQuery executes a parsed SQL query against the data dictionary
func ExecuteQuery(query *SQLQuery, dataDictionary map[string]map[string]string) ([]map[string]string, error) {
	if query.FromTable != "BB_ASSETS" {
		return nil, fmt.Errorf("unknown table: %s", query.FromTable)
	}
	
	var results []map[string]string
	
	// Filter the data based on the WHERE clause
	for _, record := range dataDictionary {
		if query.HasWhere {
			whereValue, exists := record[query.WhereColumn]
			if !exists {
				continue
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
				continue
			}
		}
		
		// Include the record in the results
		if query.SelectColumns[0] == "*" {
			// Select all columns
			results = append(results, record)
		} else {
			// Select specific columns
			selectedRecord := make(map[string]string)
			for _, col := range query.SelectColumns {
				if value, exists := record[col]; exists {
					selectedRecord[col] = value
				}
			}
			results = append(results, selectedRecord)
		}
	}
	
	return results, nil
}
