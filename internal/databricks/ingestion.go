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
	start := time.Now() 

	if err := c.ensureTableExists(ctx, req); err != nil {
		return &IngestionResult{
			TableName: req.TableName,        
			Status:    "failed",               
			Error:     err,                   
			Duration:  time.Since(start),    
		}, fmt.Errorf("failed to ensure table exists: %w", err)
	}

	if req.SampleData != "" && req.Metadata["mode"] == "mock_data" {
		rowsInserted, err := c.insertMockData(ctx, req)
		if err != nil {
			return &IngestionResult{
				TableName: req.TableName,
				Status:    "failed",        
				Error:     err,               
				Duration:  time.Since(start), 
			}, fmt.Errorf("failed to insert mock data: %w", err)
		}

		_, err = c.getRowCount(ctx, req.TableName)
		if err != nil {
			log.Printf("Could not get row count from table, using inserted count: %v", err)
		}

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

	return nil, fmt.Errorf("real BLADE ingestion not implemented - use mock data mode for POC")
}

func (c *Client) insertMockData(ctx context.Context, req *IngestionRequest) (int64, error) {
	var records []map[string]interface{} 
	
	if err := json.Unmarshal([]byte(req.SampleData), &records); err != nil {
		return 0, fmt.Errorf("failed to parse sample data: %w", err)
	}

	var values []string
	batchID := fmt.Sprintf("%d", time.Now().Unix())
	log.Printf("Preparing to insert %d records into %s.%s.%s", len(records), c.catalog, c.schema, req.TableName)
	
	for _, record := range records {
		rawDataJSON, _ := json.Marshal(record) 
		rawDataEscaped := strings.ReplaceAll(string(rawDataJSON), "'", "''")
		
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

	if err != nil {
		return 0, fmt.Errorf("failed to insert mock data batch: %w", err)
	}

	if resp.Status != nil && resp.Status.State == sql.StatementStatePending {
		log.Printf("Data insertion pending")
	}
	
	log.Printf("INSERT execution completed with status: %v", resp.Status.State)

	return int64(len(records)), nil 
}