package databricks

import (
	"context"
	"fmt"
	"log"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/sql"
	"databricks-blade-poc/internal/config"
)

type Client struct {
	workspace *databricks.WorkspaceClient
	warehouseID string
	catalog string
	schema string
}

func NewClient(cfg *config.Config) (*Client, error) {
	w, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host: cfg.DatabricksHost,
		Token: cfg.DatabricksToken,
	})

	if err != nil {
		return nil, fmt.Errorf("Failed to create the databricks client: %w", err)
	}

	return &Client{
		workspace: w,
		warehouseID: cfg.WarehouseID,
		catalog: cfg.CatalogName,
		schema: cfg.SchemaName,
	}, nil
}

func (c *Client) TestConnection(ctx context.Context) error {
	testSQL := "SELECT 1 as test"
	
	resp, err := c.workspace.StatementExecution.ExecuteStatement(
		ctx,
		sql.ExecuteStatementRequest{
			Statement:   testSQL,
			WarehouseId: c.warehouseID,
			WaitTimeout: "10s",
		},
	)
	
	if err != nil {
		return fmt.Errorf("failed to test Databricks connection: %w", err)
	}
	
	if resp.Status != nil {
		fmt.Printf("Connection test status: %v\n", resp.Status.State)
	}
	
	return nil
}

func (c *Client) ensureCatalogAndSchema(ctx context.Context) error {
	createCatalogSQL := fmt.Sprintf("CREATE CATALOG IF NOT EXISTS %s", c.catalog)
	log.Printf("Creating catalog with SQL: %s", createCatalogSQL)
	
	_, err := c.workspace.StatementExecution.ExecuteStatement(
		ctx,
		sql.ExecuteStatementRequest{
			Statement:   createCatalogSQL,
			WarehouseId: c.warehouseID,
			WaitTimeout: "30s",
		},
	)
	
	if err != nil {
		return fmt.Errorf("failed to create catalog %s: %w", c.catalog, err)
	}
	log.Printf("Successfully created/verified catalog: %s", c.catalog)
	
	createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", c.catalog, c.schema)
	log.Printf("Creating schema with SQL: %s", createSchemaSQL)
	
	_, err = c.workspace.StatementExecution.ExecuteStatement(
		ctx,
		sql.ExecuteStatementRequest{
			Statement:   createSchemaSQL,
			WarehouseId: c.warehouseID,
			WaitTimeout: "30s",
		},
	)
	
	if err != nil {
		return fmt.Errorf("failed to create schema %s.%s: %w", c.catalog, c.schema, err)
	}
	log.Printf("Successfully created/verified schema: %s.%s", c.catalog, c.schema)
	
	return nil
}

func (c *Client) ensureTableExists(ctx context.Context, req *IngestionRequest) error {
	if err := c.ensureCatalogAndSchema(ctx); err != nil {
		return err
	}
	
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s.%s.%s (
			item_id STRING,
			item_type STRING,
			classification_marking STRING,
			timestamp TIMESTAMP,
			data_source STRING,
			raw_data STRING,
			ingestion_timestamp TIMESTAMP,
			metadata MAP<STRING, STRING>
		)
	`, c.catalog, c.schema, req.TableName)
	log.Printf("Creating table with SQL: %s", createTableSQL)

	resp, err := c.workspace.StatementExecution.ExecuteStatement(
		ctx,
		sql.ExecuteStatementRequest{ 
			Statement:   createTableSQL,   
			WarehouseId: c.warehouseID,  
			Catalog:     c.catalog,     
			Schema:      c.schema,       
			WaitTimeout: "30s",   
		},
	)

	if err != nil {
		return fmt.Errorf("Failed to create table %s: %w", req.TableName, err)
	}

	if resp.Status != nil && resp.Status.State == sql.StatementStatePending {
		fmt.Printf("Table creation pending for %s\n", req.TableName)
	}

	return nil
}


func (c *Client) getRowCount(ctx context.Context, tableName string) (int64, error) {
	countSQL := fmt.Sprintf("SELECT COUNT(*) as row_count FROM %s.%s.%s", c.catalog, c.schema, tableName)

	resp, err := c.workspace.StatementExecution.ExecuteStatement(
		ctx,
		sql.ExecuteStatementRequest{
			WarehouseId: c.warehouseID,
  			Catalog: c.catalog,
  			Schema: c.schema,
  			Statement: countSQL,
			WaitTimeout: "30s",
		},
	)

	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err);
	}

	if resp.Status != nil {
		fmt.Printf("Row count query status: %v\n", resp.Status.State)
	}
	
	return 0, nil
}