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
	// Purpose: Creates the core Databricks workspace client using the official SDK.

	// Configuration Source:
	// - cfg.DatabricksHost: From DATABRICKS_HOST environment variable
	// 	- Example: "https://dbc-a1b2c3d4-e5f6.cloud.databricks.com"
	// - cfg.DatabricksToken: From DATABRICKS_TOKEN environment variable
	// 	- Example: "dapi123abc456def789ghi012jkl345mno"

	// SDK Authentication:
	// - Uses Personal Access Token authentication method
	// - SDK handles HTTPS requests, token headers, and API versioning automatically
	// - Validates token format and host URL structure
	w, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host: cfg.DatabricksHost,
		Token: cfg.DatabricksToken,
	})

	// Common Error Scenarios:
	// - Invalid Host URL: Malformed or unreachable Databricks workspace URL
	// - Authentication Failure: Invalid or expired access token
	// - Network Issues: DNS resolution or connectivity problems
	// - SDK Issues: Databricks SDK initialization problems

	// Error Wrapping:
	// - Uses %w verb to preserve original error for unwrapping
	// - Adds context about which operation failed
	// - Enables error chain inspection with errors.Is() and errors.As()
	if err != nil {
		return nil, fmt.Errorf("Failed to create the databricks client: %w", err)
	}

	// Field Population:
	// - workspace: The authenticated SDK client for all API operations
	// - warehouseID: From DATABRICKS_WAREHOUSE_ID env var
	// 	- Example: "abc123def456ghi789"
	// 	- Purpose: SQL warehouse for query execution
	// - catalog: From DATABRICKS_CATALOG env var (default: "blade_poc")
	// 	- Purpose: Top-level namespace for database objects
	// - schema: From DATABRICKS_SCHEMA env var (default: "logistics")
	// 	- Purpose: Second-level namespace within catalog
	return &Client{
		workspace: w,
		warehouseID: cfg.WarehouseID,
		catalog: cfg.CatalogName,
		schema: cfg.SchemaName,
	}, nil
}

func (c *Client) TestConnection(ctx context.Context) error {
	// Purpose: Defines minimal SQL statement to validate connectivity.

	// Why This Query:
	// - Simplest Possible: No table dependencies, no complex operations
	// - Universal: Works on any SQL engine (Databricks, MySQL, PostgreSQL, etc.)
	// - Fast Execution: Minimal processing time and resource usage
	// - Deterministic Result: Always returns same result if connection works
	// - No Side Effects: Doesn't modify any data or schema
	testSQL := "SELECT 1 as test"
	
	// ExecuteStatement Method:
	// - Uses Databricks SQL Execution API
	// - Synchronous execution with timeout
	// - Returns response with execution status and results

	// Request Parameters:
	// - Statement: The SQL to execute ("SELECT 1 as test")
	// - WarehouseId: SQL warehouse for query execution (from client config)
	// - WaitTimeout: Maximum time to wait for completion ("10s")

	// Context Usage:
	// - Enables caller to cancel operation early
	// - Provides timeout control beyond the 10s statement timeout
	// - Propagates cancellation through call chain
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
	// SQL Generation:
	// - Uses client's configured catalog name (e.g., "blade_poc")
	// - Generated SQL: "CREATE CATALOG IF NOT EXISTS blade_poc"
	// - IF NOT EXISTS: Prevents errors if catalog already exists
	// - Logging: Shows exact SQL for debugging and audit trail
	createCatalogSQL := fmt.Sprintf("CREATE CATALOG IF NOT EXISTS %s", c.catalog)
	log.Printf("Creating catalog with SQL: %s", createCatalogSQL)
	
	// Execution Details:
	// - Statement: The generated CREATE CATALOG SQL
	// - WarehouseId: SQL warehouse for DDL execution
	// - WaitTimeout: 30 seconds (longer than connection test due to DDL complexity)

	// Error Handling:
	// - Returns immediately if catalog creation fails
	// - Wraps error with specific catalog name for context
	// - Prevents schema creation if catalog fails

	// Success Logging:
	// - Confirms catalog exists (either created or already existed)
	// - Uses "created/verified" to indicate both scenarios
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
	
	// SQL Generation:
	// - Uses both catalog and schema names from client config
	// - Generated SQL: "CREATE SCHEMA IF NOT EXISTS blade_poc.logistics"
	// - Two-part naming: catalog.schema format required by Databricks
	// - IF NOT EXISTS: Safe to run multiple times
	createSchemaSQL := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s", c.catalog, c.schema)
	log.Printf("Creating schema with SQL: %s", createSchemaSQL)
	
	// Execution Details:
	// - Same pattern as catalog creation
	// - 30-second timeout for DDL operations
	// - Full schema path in error messages

	// Success Flow:
	// - Logs successful schema creation/verification
	// - Returns nil to indicate both operations succeeded
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
	// Dependency Chain:
	// - Ensures catalog exists before creating schema
	// - Ensures schema exists before creating table
	// - Fails fast: Returns immediately if parent structure creation fails

	// What This Validates:
	// - Catalog blade_poc exists
	// - Schema blade_poc.logistics exists
	// - Proper permissions for DDL operations
	if err := c.ensureCatalogAndSchema(ctx); err != nil {
		return err
	}
	
	// SQL Template Breakdown:
	// 	Three-Part Table Name:
	// 	- %s.%s.%s → blade_poc.logistics.blade_maintenance_data
	// 	- catalog.schema.table format required by Databricks Unity Catalog
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

	// Request Parameters:
	// - Statement: The generated CREATE TABLE SQL
	// - WarehouseId: SQL warehouse for DDL execution
	// - Catalog/Schema: Explicit context (redundant with SQL but required by API)
	// - WaitTimeout: 30 seconds for DDL completion

	// Why Context Parameters:
	// - Databricks API requires explicit catalog/schema context
	// - Ensures operation executes in correct namespace
	// - Provides additional validation beyond SQL statement
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

	// Error Scenarios:
	// - SQL Syntax Errors: Malformed CREATE TABLE statement
	// - Permission Issues: Insufficient rights to create tables
	// - Resource Constraints: Warehouse unavailable or overloaded
	// - Name Conflicts: Invalid table name format
	// - Network Issues: Connection problems during execution

	// Error Context:
	// - Includes specific table name that failed
	// - Preserves original error for debugging
	// - Distinguishes table creation from catalog/schema errors
	if err != nil {
		return fmt.Errorf("Failed to create table %s: %w", req.TableName, err)
	}

	// Status Monitoring:
	// - PENDING: DDL operation still running (common for large tables)
	// - SUCCEEDED: Table creation completed
	// - FAILED: Creation failed (would have returned error above)

	// Why Monitor Pending:
	// - DDL operations can be asynchronous in Databricks
	// - Provides user feedback for long-running operations
	// - Helps distinguish between network delays vs. actual processing time
	if resp.Status != nil && resp.Status.State == sql.StatementStatePending {
		fmt.Printf("Table creation pending for %s\n", req.TableName)
	}

	// - Table exists and is ready for data insertion
  	// - All prerequisites (catalog, schema) also verified
	return nil
}


func (c *Client) getRowCount(ctx context.Context, tableName string) (int64, error) {
	// SQL Generation:
	// - Uses client's configured catalog and schema names
	// - Generated SQL Example: "SELECT COUNT(*) as row_count FROM blade_poc.logistics.blade_maintenance_data"
	// - Three-part naming: Required by Databricks Unity Catalog
	// - Column alias: row_count for clear result identification
	countSQL := fmt.Sprintf("SELECT COUNT(*) as row_count FROM %s.%s.%s", c.catalog, c.schema, tableName)

	// Request Parameters:
	// - WarehouseId: SQL warehouse for query execution
	// - Catalog/Schema: Explicit context for the operation
	// - Statement: The generated COUNT query
	// - WaitTimeout: 30 seconds for query completion

	// Parameter Order Note:
	// - Statement comes after context parameters (different from other functions)
	// - Still functionally equivalent
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

	// Catches SQL Execution Errors:
	// - Table not found
	// - Permission issues
	// - Network problems
	// - Warehouse unavailable
	// - Query timeoutugging
	// - Returns 0 count on any failure
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %w", err);
	}

	// Status Monitoring:
	// - SUCCEEDED: Query completed successfully
	// - FAILED: Query execution failed
	// - PENDING: Query still running (unlikely with simple COUNT)

	// Purpose:
	// - Provides visibility into query execution
	// - Helps debug performance or execution issues
	// - Confirms successful operation completion
	if resp.Status != nil {
		fmt.Printf("Row count query status: %v\n", resp.Status.State)
	}

	// Structure Validation:
	// - resp.Result != nil: Ensures response contains result data
	// - len(resp.Result.DataArray) > 0: Confirms at least one row returned
	// - len(resp.Result.DataArray[0]) > 0: Validates first row has columns
	if resp.Result != nil && len(resp.Result.DataArray) > 0 && len(resp.Result.DataArray[0]) > 0 {
		// Databricks Result Format:
		// resp.Result.DataArray = [
		// 	["42"],  // First (and only) row with COUNT result
		// ]
		// - DataArray[0]: First row of results
		// - DataArray[0][0]: First column (the COUNT value) as string
		countStr := resp.Result.DataArray[0][0]

		// String to Integer Conversion:
		// - countStr: Raw count value as string (e.g., "42")
		// - strconv.ParseInt(countStr, 10, 64): Converts to int64
		// 	- Base 10 (decimal)
		// 	- 64-bit integer
		// 	- Handles large row counts
		count, parseErr := strconv.ParseInt(countStr, 10, 64)

		// Parse Error Handling:
		// - Returns specific error if count can't be parsed as integer
		// - Includes the problematic string value for debugging
		if parseErr != nil {
			return 0, fmt.Errorf("failed to parse row count '%s': %w", countStr, parseErr)
		}
		// Success Logging:
		// - Logs the actual count with full table path
		// - Example: "Table blade_poc.logistics.blade_maintenance_data contains 5 rows"
		log.Printf("Table %s.%s.%s contains %d rows", c.catalog, c.schema, tableName, count)
		return count, nil
	}

	
	return 0, nil
}