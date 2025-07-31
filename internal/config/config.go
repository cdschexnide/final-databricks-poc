package config

import (
	"os"
	"github.com/joho/godotenv"
)

type Config struct {
	DatabricksHost string
	DatabricksToken string
	WarehouseID string
	CatalogName string
	SchemaName string

	BLADEDataPath string
	BLADEDataSource string
}

func LoadConfig() (*Config, error) {
	_ = godotenv.Load(".env")

	return &Config{
		DatabricksHost: os.Getenv("DATABRICKS_HOST"),
		DatabricksToken: os.Getenv("DATABRICKS_TOKEN"),
		WarehouseID: os.Getenv("DATABRICKS_WAREHOUSE_ID"),
		CatalogName: getEnvOrDefault("DATABRICKS_CATALOG", "blade_poc"),
		SchemaName: getEnvOrDefault("DATABRICKS_SCHEMA", "logistics"),

		// hardcoded for PoC
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