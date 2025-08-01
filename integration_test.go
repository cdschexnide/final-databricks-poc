package main

//   Test Package: Uses package main to test the main application
//   Standard Testing: testing package for Go test framework
//   Internal Dependencies: All three core packages for end-to-end testing
import (
	"context"
	"os"
	"testing"
	"time"

	"databricks-blade-poc/internal/blade"
	"databricks-blade-poc/internal/config"
	"databricks-blade-poc/internal/databricks"
)

// Purpose: Tests all 8 combinations of BLADE data types and formats from the mock data
func TestBLADEIngestionIntegration(t *testing.T) {
	// Configuration Validation:
	// - Loads environment-based configuration
	// - Skips tests if required Databricks credentials are missing
	// - Creates and tests Databricks client connection

	cfg, err := config.LoadConfig()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	if cfg.DatabricksHost == "" || cfg.DatabricksToken == "" || cfg.WarehouseID == "" {
		t.Skip("Databricks environment variables not set, skipping integration tests")
	}

	dbClient, err := databricks.NewClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create Databricks client: %v", err)
	}

	ctx := context.Background()
	if err := dbClient.TestConnection(ctx); err != nil {
		t.Fatalf("Failed to connect to Databricks: %v", err)
	}

	// Complete Coverage Matrix:
	// - 4 Data Types: maintenance, sortie, deployment, logistics
	// - 2 Formats: JSON, CSV
	// - 8 Total Combinations: Every possible input combination

	// Subtest Execution:
	// - Each combination runs as a separate subtest
	// - Parallel execution possible
	// - Individual test result reporting
	bladeAdapter := blade.NewBLADEAdapter(cfg.BLADEDataSource, cfg.BLADEDataPath)

	testCases := []struct {
		name     string
		dataType string
		format   string
	}{
		{"Maintenance JSON", "maintenance", "JSON"},
		{"Maintenance CSV", "maintenance", "CSV"},
		{"Sortie JSON", "sortie", "JSON"},
		{"Sortie CSV", "sortie", "CSV"},
		{"Deployment JSON", "deployment", "JSON"},
		{"Deployment CSV", "deployment", "CSV"},
		{"Logistics JSON", "logistics", "JSON"},
		{"Logistics CSV", "logistics", "CSV"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testBLADEIngestion(t, bladeAdapter, dbClient, tc.dataType, tc.format)
		})
	}
}

//   Three-Phase Testing:
//   1. Request Preparation: Tests BLADE adapter functionality
//   2. Request Validation (validateIngestionRequest): Validates request structure and content
//   3. Ingestion Execution: Tests Databricks operations
//   4. Result Validation (validateIngestionResult): Validates response structure and metrics

//   Timing Measurement: Tracks test execution time for performance monitoring

func testBLADEIngestion(t *testing.T, adapter *blade.BLADEAdapter, client *databricks.Client, dataType, format string) {
	ctx := context.Background()
	start := time.Now()

	t.Logf("Testing %s data type with %s format", dataType, format)

	req, err := adapter.PrepareIngestionRequest(dataType, format)
	if err != nil {
		t.Fatalf("Failed to prepare ingestion request for %s/%s: %v", dataType, format, err)
	}

	validateIngestionRequest(t, req, dataType, format)

	result, err := client.IngestBLADEData(ctx, req)
	if err != nil {
		t.Fatalf("Ingestion failed for %s/%s: %v", dataType, format, err)
	}

	validateIngestionResult(t, result, dataType, format, start)

	t.Logf("Successfully ingested %s/%s: %d rows in %v", 
		dataType, format, result.RowsIngested, result.Duration)
}

func validateIngestionRequest(t *testing.T, req *databricks.IngestionRequest, dataType, format string) {
	// Comprehensive Request Validation:

	// Structural Validation:
	// - Request object is not nil
	// - All required fields are populated
	// - No empty critical values
	if req == nil {
		t.Fatal("Ingestion request is nil")
	}

	if req.TableName == "" {
		t.Error("TableName is empty")
	}

	if req.SourcePath == "" {
		t.Error("SourcePath is empty")
	}

	if req.DataSource == "" {
		t.Error("DataSource is empty")
	}

	if req.SampleData == "" {
		t.Error("SampleData is empty")
	}

	if req.Metadata == nil {
		t.Error("Metadata is nil")
	}

	// Content Consistency:
	// - Metadata data_type matches input parameter
	// - Metadata original_format matches input parameter
	// - SampleData is populated for mock mode
	expectedMetadata := []string{"source_system", "data_type", "integration", "description", "mode", "original_format"}
	for _, field := range expectedMetadata {
		if _, exists := req.Metadata[field]; !exists {
			t.Errorf("Missing metadata field: %s", field)
		}
	}

	if req.Metadata["data_type"] != dataType {
		t.Errorf("Metadata data_type (%s) doesn't match expected (%s)", req.Metadata["data_type"], dataType)
	}

	if req.Metadata["original_format"] != format {
		t.Errorf("Metadata original_format (%s) doesn't match expected (%s)", req.Metadata["original_format"], format)
	}
}

func validateIngestionResult(t *testing.T, result *databricks.IngestionResult, dataType, format string, startTime time.Time) {
	// Result Quality Checks:

	// Success Indicators:
	// - Status is "completed"
	// - No error field present
	// - Positive row count
	// - Reasonable execution duration
	if result == nil {
		t.Fatal("Ingestion result is nil")
	}

	if result.Status != "completed" {
		t.Errorf("Expected status 'completed', got '%s'", result.Status)
	}

	if result.Error != nil {
		t.Errorf("Unexpected error in result: %v", result.Error)
	}

	if result.RowsIngested <= 0 {
		t.Errorf("Expected positive rows ingested, got %d", result.RowsIngested)
	}

	if result.Duration <= 0 {
		t.Errorf("Expected positive duration, got %v", result.Duration)
	}

	if result.TableName == "" {
		t.Error("TableName is empty in result")
	}

	if result.Metadata == nil {
		t.Error("Result metadata is nil")
	}

	// Performance Validation
	if result.Duration < time.Millisecond {
		t.Errorf("Ingestion completed suspiciously fast: %v", result.Duration)
	}

	// Data Integrity:
	// - Row count is positive
	// - Duration is reasonable (not too fast/slow)
	// - Metadata is populated
	if time.Since(startTime) < result.Duration {
		t.Logf("Warning: Result duration (%v) seems longer than actual time elapsed (%v)", 
			result.Duration, time.Since(startTime))
	}
}

func TestBLADEAdapterMappings(t *testing.T) {
	// Configuration Validation:
	// - Tests adapter initialization
	// - Validates all expected data types are supported
	// - Ensures no missing or extra data types
	cfg, err := config.LoadConfig()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	adapter := blade.NewBLADEAdapter(cfg.BLADEDataSource, cfg.BLADEDataPath)
	
	expectedDataTypes := []string{"maintenance", "sortie", "deployment", "logistics"}
	supportedTypes := adapter.GetSupportedDataTypes()

	if len(supportedTypes) != len(expectedDataTypes) {
		t.Errorf("Expected %d supported data types, got %d", len(expectedDataTypes), len(supportedTypes))
	}

	supportedMap := make(map[string]bool)
	for _, dataType := range supportedTypes {
		supportedMap[dataType] = true
	}

	for _, expected := range expectedDataTypes {
		if !supportedMap[expected] {
			t.Errorf("Expected data type '%s' not found in supported types: %v", expected, supportedTypes)
		}
	}
}

// Error Handling Tests
//   Negative Testing:
//   - Tests invalid data type handling
//   - Tests invalid format handling
//   - Ensures proper error responses for bad inputs
func TestInvalidDataType(t *testing.T) {
	cfg, err := config.LoadConfig()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	adapter := blade.NewBLADEAdapter(cfg.BLADEDataSource, cfg.BLADEDataPath)
	
	_, err = adapter.PrepareIngestionRequest("invalid_type", "JSON")
	if err == nil {
		t.Error("Expected error for invalid data type, got nil")
	}
}

func TestInvalidFormat(t *testing.T) {
	cfg, err := config.LoadConfig()
	if err != nil {
		t.Fatalf("Failed to load configuration: %v", err)
	}

	adapter := blade.NewBLADEAdapter(cfg.BLADEDataSource, cfg.BLADEDataPath)
	
	_, err = adapter.PrepareIngestionRequest("maintenance", "XML")
	if err == nil {
		t.Error("Expected error for invalid format, got nil")
	}
}

// Performance Benchmarking Tests
//   Performance Testing:
//   - Benchmarks complete ingestion workflow
//   - Measures operations per second
//   - Excludes setup time with b.ResetTimer()
//   - Runs multiple iterations for statistical accuracy
func BenchmarkBLADEIngestion(b *testing.B) {
	if os.Getenv("CI") != "" {
		b.Skip("Skipping benchmark tests in CI environment")
	}

	cfg, err := config.LoadConfig()
	if err != nil {
		b.Fatalf("Failed to load configuration: %v", err)
	}

	if cfg.DatabricksHost == "" || cfg.DatabricksToken == "" || cfg.WarehouseID == "" {
		b.Skip("Databricks environment variables not set, skipping benchmark tests")
	}

	dbClient, err := databricks.NewClient(cfg)
	if err != nil {
		b.Fatalf("Failed to create Databricks client: %v", err)
	}

	adapter := blade.NewBLADEAdapter(cfg.BLADEDataSource, cfg.BLADEDataPath)
	ctx := context.Background()

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		req, err := adapter.PrepareIngestionRequest("maintenance", "JSON")
		if err != nil {
			b.Fatalf("Failed to prepare request: %v", err)
		}

		_, err = dbClient.IngestBLADEData(ctx, req)
		if err != nil {
			b.Fatalf("Ingestion failed: %v", err)
		}
	}
}