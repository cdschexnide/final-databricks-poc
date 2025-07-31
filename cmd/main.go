package main

import (
	"context" // For cancellation and timeout control
	"fmt" // For formatted output and string operations
	"log" // For logging messages and fatal errors
	"strings" // For string manipulation (result formatting)
	"os" // For command-line argument access
	"databricks-blade-poc/internal/blade" // BLADE data type handling and file processing
	"databricks-blade-poc/internal/config" // Environment variable configuration management
	"databricks-blade-poc/internal/databricks" // Databricks client and ingestion operations
)

func main() {
	// Purpose: Creates base context for all operations
	// Usage: Passed to Databricks operations for cancellation/timeout control
	// Future Enhancement: Could add timeout or cancellation handling
	ctx := context.Background()

	// Configuration Source:
	// - Loads from .env file if present
	// - Falls back to environment variables
	// - Uses defaults for optional settings

	// Error Handling:
	// - Fatal exit if configuration loading fails
	// - Prevents proceeding with invalid/missing config
	cfg, err := config.LoadConfig()

	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Required Variables Checked:
	// - DATABRICKS_HOST: Workspace URL
	// - DATABRICKS_TOKEN: Authentication token
	// - DATABRICKS_WAREHOUSE_ID: SQL warehouse identifier

	// Validation Logic: All three must be non-empty strings
	// Error Message: Directs user to check .env file
	if cfg.DatabricksHost == "" || cfg.DatabricksToken == "" || cfg.WarehouseID == "" {
		log.Fatal("The required Databricks environment variables are missing. Check your .env file")
	}

	// Client Initialization:
	// - Creates authenticated Databricks workspace client
	// - Configures warehouse, catalog, and schema settings
	// - Handles SDK initialization and authentication

	// Error Scenarios:
	// - Invalid host URL format
	// - Authentication failures
	// - Network connectivity issues
	dbClient, err := databricks.NewClient(cfg)

	if err != nil {
		log.Fatalf("Failed to create Databricks client: %v", err)
	}

	// Pre-flight Validation:
	// - Executes simple SELECT 1 query
	// - Validates authentication and warehouse accessibility
	// - Provides immediate feedback on connection status

	// User Experience:
	// - Shows "Testing..." message for user awareness
	// - Confirms successful connection before proceeding
	// - Fails fast if Databricks is unreachable
	log.Println("Testing Databricks connection...")
	if err := dbClient.TestConnection(ctx); err != nil {
		log.Fatalf("Failed to connect to Databricks: %v", err)
	}
	log.Println("Successfully connected to Databricks")

	// Adapter Configuration:
	// - DataSource: "BLADE_LOGISTICS" (from config)
	// - DataPath: "mock_blade_data/" (from config)

	// Initialization Process:
	// - Loads all 4 BLADE data type mappings
	// - Indexes them by data type for fast lookup
	// - Shows supported types for user reference
	bladeAdapter := blade.NewBLADEAdapter(
		cfg.BLADEDataSource,
		cfg.BLADEDataPath,
	)

	log.Printf("Supported BLADE data types: %v", bladeAdapter.GetSupportedDataTypes())

	// Default Values:
	// - dataType: "maintenance" if not specified
	// - format: "JSON" if not specified

	// Argument Processing:
	// - os.Args[1]: Data type (maintenance, sortie, deployment, logistics)
	// - os.Args[2]: Format (JSON or CSV, case-insensitive)

	// Format Validation:
	// - Converts to uppercase for consistency
	// - Validates against allowed values
	// - Fatal error for invalid formats
	dataType := "maintenance"
	format := "JSON"
	
	if len(os.Args) > 1 {
		dataType = os.Args[1]
	}
	
	if len(os.Args) > 2 {
		format = strings.ToUpper(os.Args[2])
		if format != "JSON" && format != "CSV" {
			log.Fatalf("Invalid format: %s. Use JSON or CSV", format)
		}
	}

	// Two-Step Process:

	// Step 1: Request Preparation
	// - Validates data type against supported mappings
	// - Loads mock data file (JSON or CSV)
	// - Converts CSV to JSON if needed
	// - Builds complete IngestionRequest with metadata

	// Step 2: Data Ingestion
	// - Ensures database structure exists (catalog → schema → table)
	// - Inserts mock data into Databricks table
	// - Verifies row count matches insertion
	// - Returns detailed IngestionResult

	// Error Handling: Fatal exit on any failure with descriptive messages

	log.Printf("Starting ingestion for BLADE data (type: %s, format: %s)", dataType, format)

	req, err := bladeAdapter.PrepareIngestionRequest(dataType, format)

	if err != nil {
		log.Fatalf("Failed to prepare ingestion request: %v", err)
	}

	result, err := dbClient.IngestBLADEData(ctx, req)

	if err != nil {
		log.Fatalf("Ingestion failed: %v", err)
	}

	// Formatted Output Design:
	// - Header/Footer: 50-character equals sign borders
	// - Separator: Dashed line under title
	// - Key Metrics: Table name, status, row count, timing
	// - Source Indicator: Clearly marks as mock data
	fmt.Printf("\n" + strings.Repeat("=", 50) + "\n")
	fmt.Printf("BLADE INGESTION RESULTS")
	fmt.Printf("\n" + strings.Repeat("-", 50) + "\n")
	fmt.Printf("Table: %s\n", result.TableName)
	fmt.Printf("Status: %s\n", result.Status)
	fmt.Printf("Rows Ingested: %d\n", result.RowsIngested)
	fmt.Printf("Duration: %s\n", result.Duration)
	fmt.Printf("Source: BLADE (mock)")
	fmt.Printf("\n" + strings.Repeat("=", 50) + "\n")
}