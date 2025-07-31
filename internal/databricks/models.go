package databricks

import (
	"encoding/json"
	"time"
)

// Encapsulates all parameters needed for a data ingestion operation.
type IngestionRequest struct {
	TableName     string            `json:"tableName"`
	SourcePath    string            `json:"sourcePath"`
	FileFormat    string            `json:"fileFormat"` // JSON or CSV
	FormatOptions string            `json:"formatOptions"`
	DataSource    string            `json:"dataSource"`  // BLADE/ADVANA
	SampleData    string            `json:"sampleData,omitempty"` // for PoC
	Metadata      map[string]string `json:"metadata"`
}

// Contains the results and statistics from a completed ingestion operation.
type IngestionResult struct {
	RowsIngested int64 `json:"rowsIngested"`
	Duration time.Duration `json:"duration"`
	TableName string `json:"tableName"`
	Status string `json:"status"`
	Error error `json:"error,omitempty"`
	Metadata map[string]interface{} `json:"metadata"`
}

// Convenience method for serializing results to JSON.
func (r *IngestionResult) ToJSON() []byte {
	data, _ := json.Marshal(r)
	return data
}

// Type-safe constants for BLADE data types.
type BLADEDataType string

const (
	MaintenanceData BLADEDataType = "maintenance"
	SortieData BLADEDataType = "sortie"
	DeploymentData BLADEDataType = "deployment"
	LogisticsData BLADEDataType = "logistics"
)