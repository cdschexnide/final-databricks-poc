package config

import (
	"os"
	"github.com/joho/godotenv"
)

type Config struct {
	// Databricks Configuration
	DatabricksHost string
	DatabricksToken string
	WarehouseID string
	CatalogName string
	SchemaName string

	// BLADE Configuration (hardcoded for POC)
	BLADEDataPath string
	BLADEDataSource string
}

func LoadConfig() (*Config, error) {
	// Load .env file if it exists (this is how infinityai-cataloger does)
	_ = godotenv.Load(".env")

	return &Config{
		DatabricksHost: os.Getenv("DATABRICKS_HOST"),
		DatabricksToken: os.Getenv("DATABRICKS_TOKEN"),
		WarehouseID: os.Getenv("DATABRICKS_WAREHOUSE_ID"),
		CatalogName: getEnvOrDefault("DATABRICKS_CATALOG", "blade_poc"),
		SchemaName: getEnvOrDefault("DATABRICKS_SCHEMA", "logistics"),

		// Hardcoded for POC - will be dynamic in integration
		BLADEDataPath: "mock_blade_data/",
		BLADEDataSource: "BLADE_LOGISTICS",
	}, nil
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value;
	}
	return defaultValue;
}