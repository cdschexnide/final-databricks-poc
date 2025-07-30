package databricks

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"github.com/databricks/databricks-sdk-go/service/sql"
)

// takes context for cancellation and request with all parameters
// returns result with statistics
func (c *Client) IngestBLADEData(ctx context.Context, req *IngestionRequest) (*IngestionResult, error) {
	start := time.Now() 

	// ensure table exists, if it doesn't exist, then it creates the table
	if err := c.ensureTableExists(ctx, req); err != nil {
		return &IngestionResult{
			TableName: req.TableName,        
			Status:    "failed",               
			Error:     err,                   
			Duration:  time.Since(start),    
		}, fmt.Errorf("failed to ensure table exists: %w", err)
	}

	// checks if this is POC mode with mock data (not real BLADE files)
	if req.SampleData != "" && req.Metadata["mode"] == "mock_data" {
		// inserts mock data directly into Databricks tables
		rowsInserted, err := c.insertMockData(ctx, req)
		if err != nil {
			return &IngestionResult{
				TableName: req.TableName,
				Status:    "failed",        
				Error:     err,               
				Duration:  time.Since(start), 
			}, fmt.Errorf("failed to insert mock data: %w", err)
		}

		// validates by counting rows in Databricks table
		rowCount, err := c.getRowCount(ctx, req.TableName)
		if err != nil {
			rowCount = rowsInserted
		}

		return &IngestionResult{
			RowsIngested: rowCount,      
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

	return nil, fmt.Errorf("real BLADE ingestion not implemented - use mock data mode for POC")
}

// takes context for cancellation and request with mock data
// returns count of rows inserted and error
func (c *Client) insertMockData(ctx context.Context, req *IngestionRequest) (int64, error) {
	var records []map[string]interface{} 
	
	// parse JSON data from the request
	if err := json.Unmarshal([]byte(req.SampleData), &records); err != nil {
		return 0, fmt.Errorf("failed to parse sample data: %w", err)
	}

	// builds INSERT statement with all records
	var values []string
	batchID := fmt.Sprintf("%d", time.Now().Unix())
	
	// transforms each JSON record into SQL VALUES
	for _, record := range records {
		// converts record back to JSON for raw_data storage
		rawDataJSON, _ := json.Marshal(record) 
		rawDataEscaped := strings.ReplaceAll(string(rawDataJSON), "'", "''")
		
		// build VALUES clause for this record
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

	// executes batch INSERT
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

	// sends SQL to Databricks
	_, err := c.workspace.StatementExecution.ExecuteStatement(
		ctx,
		sql.ExecuteStatementRequest{ 
			Statement:   insertSQL,   
			WarehouseId: c.warehouseID,  
			Catalog:     c.catalog,     
			Schema:      c.schema,       
			WaitTimeout: "30s",   
		},
	)

	if err != nil {
		return 0, fmt.Errorf("failed to insert mock data batch: %w", err)
	}

	// returns count of records we inserted
	return int64(len(records)), nil 
}