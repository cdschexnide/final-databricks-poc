package databricks

import (
	"encoding/json"
	"time"
)

// a request to ingest data into Databricks
type IngestionRequest struct {
	TableName     string            `json:"tableName"`
	SourcePath    string            `json:"sourcePath"`
	FileFormat    string            `json:"fileFormat"` // JSON or CSV
	FormatOptions string            `json:"formatOptions"`
	DataSource    string            `json:"dataSource"`  // BLADE/ADVANA
	SampleData    string            `json:"sampleData,omitempty"` // For mock POC mode
	Metadata      map[string]string `json:"metadata"`
}

// the result of a data ingestion operation
type IngestionResult struct {
	RowsIngested int64 `json:"rowsIngested"`
	Duration time.Duration `json:"duration"`
	TableName string `json:"tableName"`
	Status string `json:"status"`
	Error error `json:"error,omitempty"`
	Metadata map[string]interface{} `json:"metadata"`
}


// converts the result to JSON for catalog integration
func (r *IngestionResult) ToJSON() []byte {
	data, _ := json.Marshal(r)
	return data
}

// represents the different types of BLADE data (mock types for now)
type BLADEDataType string

const (
	MaintenanceData BLADEDataType = "maintenance"
	SortieData BLADEDataType = "sortie"
	DeploymentData BLADEDataType = "deployment"
	LogisticsData BLADEDataType = "logistics"
)