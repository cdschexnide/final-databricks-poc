package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
	"github.com/databricks/databricks-sdk-go/service/sql"
)


func (c *Client) IngestBLADEData(ctx context.Context, req *IngestionRequest) (*IngestionResult, error) {
	// - Captures start time to measure total ingestion duration
  	// - Used in all return paths to provide accurate timing
	start := time.Now() 

	// - Calls ensureTableExists() which:
    // - Creates catalog if missing (CREATE CATALOG IF NOT EXISTS blade_poc)
    // - Creates schema if missing (CREATE SCHEMA IF NOT EXISTS blade_poc.logistics)
    // - Creates table with standardized schema (item_id, item_type, classification_marking, etc.)
    // - Returns detailed failure result if table creation fails
	if err := c.ensureTableExists(ctx, req); err != nil {
		return &IngestionResult{
			TableName: req.TableName,        
			Status:    "failed",               
			Error:     err,                   
			Duration:  time.Since(start),    
		}, fmt.Errorf("failed to ensure table exists: %w", err)
	}

	// - Checks two conditions for POC mode:
    // - SampleData field contains JSON data (from BLADE adapter)
    // - Metadata explicitly marks this as "mock_data" mode
  	// - This is the main execution path for the current POC
	if req.SampleData != "" && req.Metadata["mode"] == "mock_data" {
		// - Delegates actual insertion to insertMockData() helper function
  		// - Returns failure result with timing if insertion fails
		rowsInserted, err := c.insertMockData(ctx, req)
		if err != nil {
			return &IngestionResult{
				TableName: req.TableName,
				Status:    "failed",        
				Error:     err,               
				Duration:  time.Since(start), 
			}, fmt.Errorf("failed to insert mock data: %w", err)
		}

		// - Tries to validate insertion by querying row count
		// - Logs warning but doesn't fail if count query fails
		// - Uses inserted count as fallback (current behavior)
		_, err = c.getRowCount(ctx, req.TableName)
		if err != nil {
			log.Printf("Could not get row count from table, using inserted count: %v", err)
		}

		// - Constructs success result with:
		// - Actual rows inserted count
		// - Total execution time
		// - Original request metadata preserved
		// - Ingestion type marked as "mock_data_insert"
		return &IngestionResult{
			RowsIngested: rowsInserted,  
			Duration:     time.Since(start),  
			TableName:    req.TableName,      
			Status:       "completed",      
			Metadata: map[string]interface{}{ 
				"source_path":    req.SourcePath,    
				"file_format":    req.FileFormat,      
				"data_source":    req.DataSource,      
				"blade_metadata": req.Metadata,      
				"ingestion_type": "mock_data_insert",  
			},
		}, nil 
	}

	// - Currently only supports mock data mode
  	// - Future enhancement would add real BLADE file processing here
	return nil, fmt.Errorf("real BLADE ingestion not implemented - use mock data mode for POC")
}

func (c *Client) insertMockData(ctx context.Context, req *IngestionRequest) (int64, error) {
	var records []map[string]interface{} 
	
	// - Declares slice to hold parsed JSON records
	// - Converts req.SampleData string to []byte for unmarshaling
	// - Parses into []map[string]interface{} - array of flexible key-value maps
	// - Returns immediately if JSON is malformed
	if err := json.Unmarshal([]byte(req.SampleData), &records); err != nil {
		return 0, fmt.Errorf("failed to parse sample data: %w", err)
	}

	// - values: Will hold SQL VALUES clauses for each record
   	// - batchID: Unix timestamp to group related inserts (for tracking/debugging)
    // - Logs insertion intent with full table path and record count
	var values []string
	batchID := fmt.Sprintf("%d", time.Now().Unix())
	log.Printf("Preparing to insert %d records into %s.%s.%s", len(records), c.catalog, c.schema, req.TableName)
	
	for _, record := range records {
		//  - Re-marshals the parsed record back to JSON string
		//  - This preserves the original structure in raw_data column
		//  - Escapes single quotes (' → '') for SQL safety
		rawDataJSON, _ := json.Marshal(record) 
		rawDataEscaped := strings.ReplaceAll(string(rawDataJSON), "'", "''")
		
		//   Maps JSON fields to standardized table schema:
		// 	- item_id, item_type, classification_marking, timestamp: Direct from JSON
		// 	- data_source: From request (e.g., "BLADE_LOGISTICS")
		// 	- raw_data: Complete escaped JSON record
		// 	- ingestion_timestamp: Current database time
		// 	- metadata: Databricks MAP with batch tracking info
		value := fmt.Sprintf(`(
			'%s',
			'%s', 
			'%s',
			TIMESTAMP '%s',
			'%s',
			'%s',
			current_timestamp(),
			map('source', 'mock_blade', 'batch_id', '%s', 'data_type', '%s')
		)`,
			record["item_id"],                  
			record["item_type"],            
			record["classification_marking"],  
			record["timestamp"],   
			req.DataSource,                     
			rawDataEscaped,             
			batchID,                        
			req.Metadata["data_type"],  
		)
		values = append(values, value)
	}

	// - Constructs complete INSERT statement
	// - Uses 3-part naming: catalog.schema.table
	// - Joins all VALUES clauses with commas for batch insert
	// - Example result: INSERT INTO blade_poc.logistics.blade_maintenance_data (...) VALUES (...), (...), (...)
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s.%s.%s (
			item_id,
			item_type,
			classification_marking,
			timestamp,
			data_source,
			raw_data,
			ingestion_timestamp,
			metadata
		) VALUES %s
	`, 
		c.catalog,    
		c.schema,   
		req.TableName, 
		strings.Join(values, ",\n")) 

	// - Logs execution attempt
	// - Calls Databricks SQL Execution API
	// - Specifies warehouse, catalog, schema context
	// - 30-second timeout for statement completion
	log.Printf("Executing INSERT statement for %d records", len(records))
	resp, err := c.workspace.StatementExecution.ExecuteStatement(
		ctx,
		sql.ExecuteStatementRequest{ 
			Statement:   insertSQL,   
			WarehouseId: c.warehouseID,  
			Catalog:     c.catalog,     
			Schema:      c.schema,       
			WaitTimeout: "30s",   
		},
	)

	// - Returns error if SQL execution failed
	// - Logs if statement is still pending (async processing)
	// - Logs final execution status
	// - Returns count of records processed (assumes all succeeded)

	if err != nil {
		return 0, fmt.Errorf("failed to insert mock data batch: %w", err)
	}

	if resp.Status != nil && resp.Status.State == sql.StatementStatePending {
		log.Printf("Data insertion pending")
	}
	
	log.Printf("INSERT execution completed with status: %v", resp.Status.State)

	return int64(len(records)), nil 
}