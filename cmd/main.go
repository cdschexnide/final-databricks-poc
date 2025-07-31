package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"os"
	"databricks-blade-poc/internal/blade"
	"databricks-blade-poc/internal/config"
	"databricks-blade-poc/internal/databricks"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help" || os.Args[1] == "help") {
		fmt.Println("Usage: go run cmd/main.go [dataType] [format]")
		fmt.Println("\nArguments:")
		fmt.Println("  dataType: maintenance, sortie, deployment, or logistics (default: maintenance)")
		fmt.Println("  format:   JSON or CSV (default: JSON)")
		fmt.Println("\nExamples:")
		fmt.Println("  go run cmd/main.go                    # Ingests maintenance data in JSON format")
		fmt.Println("  go run cmd/main.go sortie             # Ingests sortie data in JSON format")
		fmt.Println("  go run cmd/main.go logistics CSV      # Ingests logistics data in CSV format")
		os.Exit(0)
	}

	ctx := context.Background()

	cfg, err := config.LoadConfig()

	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	if cfg.DatabricksHost == "" || cfg.DatabricksToken == "" || cfg.WarehouseID == "" {
		log.Fatal("The required Databricks environment variables are missing. Check your .env file")
	}

	dbClient, err := databricks.NewClient(cfg)

	if err != nil {
		log.Fatalf("Failed to create Databricks client: %v", err)
	}

	log.Println("Testing Databricks connection...")
	if err := dbClient.TestConnection(ctx); err != nil {
		log.Fatalf("Failed to connect to Databricks: %v", err)
	}
	log.Println("Successfully connected to Databricks")

	// translates the BLADE types to Databricks tables
	bladeAdapter := blade.NewBLADEAdapter(
		cfg.BLADEDataSource,
		cfg.BLADEDataPath,
	)

	log.Printf("Supported BLADE data types: %v", bladeAdapter.GetSupportedDataTypes())

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

	log.Printf("Starting ingestion for BLADE data (type: %s, format: %s)", dataType, format)

	req, err := bladeAdapter.PrepareIngestionRequest(dataType, format)

	if err != nil {
		log.Fatalf("Failed to prepare ingestion request: %v", err)
	}

	result, err := dbClient.IngestBLADEData(ctx, req)

	if err != nil {
		log.Fatalf("Ingestion failed: %v", err)
	}

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