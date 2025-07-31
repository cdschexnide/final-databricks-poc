package blade

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"databricks-blade-poc/internal/databricks"
)

type BLADEAdapter struct {
	dataSource string // a specific BLADE deployment
	basePath string // the root volume path where BLADE stores data files
	mappings map[string]BLADEDataMapping // map of data type -> table configuration (for quick lookup)
}

func NewBLADEAdapter(dataSource, basePath string) *BLADEAdapter {
	mappings := make(map[string]BLADEDataMapping)

	for _, mapping := range GetBLADEMappings() {
		mappings[mapping.DataType] = mapping
	}

	return &BLADEAdapter{
		dataSource: dataSource,
		basePath:   basePath,
		mappings:   mappings,
	}
}

func (b *BLADEAdapter) PrepareIngestionRequest(dataType string, format string) (*databricks.IngestionRequest, error) {
	mapping, exists := b.mappings[dataType]

	if !exists {
		return nil, fmt.Errorf("Unsupported BLADE data type: %s", dataType)
	}

	if format == "" {
		format = "JSON"
	}

	var sampleData string
	var err error
	
	switch format {
	case "JSON":
		sampleData, err = b.loadMockDataFile(dataType)
	case "CSV":
		sampleData, err = b.loadMockCSVAsJSON(dataType)
	default:
		return nil, fmt.Errorf("Unsupported format: %s. Use JSON or CSV", format)
	}
	
	if err != nil {
		return nil, fmt.Errorf("failed to load mock data for %s: %w", dataType, err)
	}

	return &databricks.IngestionRequest{
		TableName:     mapping.TableName,
		SourcePath:    "mock://" + dataType,
		FileFormat:    "JSON", 
		FormatOptions: "'multiLine' = 'true', 'inferSchema' = 'true'",
		DataSource:    b.dataSource,
		SampleData:    sampleData,
		Metadata: map[string]string{
			"source_system": "BLADE",
			"data_type":     dataType,
			"integration":   "databricks_poc",
			"description":   mapping.Description,
			"mode":          "mock_data",
			"original_format": format,
		},
	}, nil
}

func (b *BLADEAdapter) GetSupportedDataTypes() []string {
	types := make([]string, 0, len(b.mappings))
	for dataType := range b.mappings {
		types = append(types, dataType)
	}
	return types
}

func (b *BLADEAdapter) loadMockDataFile(dataType string) (string, error) {
	fileName := fmt.Sprintf("%s_data.json", dataType)
	filePath := filepath.Join(b.basePath, dataType, fileName)
	
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read mock data file %s: %w", filePath, err)
	}
	
	return string(data), nil
}

func (b *BLADEAdapter) loadMockCSVAsJSON(dataType string) (string, error) {
	fileName := fmt.Sprintf("%s_data.csv", dataType)
	filePath := filepath.Join(b.basePath, dataType, fileName)
	
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open CSV file %s: %w", filePath, err)
	}
	defer file.Close()
	
	reader := csv.NewReader(file)

	records, err := reader.ReadAll()
	if err != nil {
		return "", fmt.Errorf("failed to read CSV file %s: %w", filePath, err)
	}
	
	if len(records) < 2 {
		return "", fmt.Errorf("CSV file %s has no data rows", filePath)
	}

	headers := records[0]
	
	var jsonRecords []map[string]interface{}
	
	for i := 1; i < len(records); i++ {
		record := make(map[string]interface{})

		for j, header := range headers {
			if j < len(records[i]) {
				value := records[i][j]
				
				if header == "parts_required" || header == "compliance_refs" {
					if value != "" {
						parts := splitAndTrim(value, ";")
						record[header] = parts
					} else {
						record[header] = []string{}
					}
				} else if value == "" {
					record[header] = nil
				} else {
					record[header] = value
				}
			}
		}
		
		jsonRecords = append(jsonRecords, record)
	}
	
	jsonData, err := json.Marshal(jsonRecords)
	if err != nil {
		return "", fmt.Errorf("failed to convert CSV to JSON: %w", err)
	}
	
	return string(jsonData), nil
}

func splitAndTrim(s string, sep string) []string {
	parts := []string{}
	splits := strings.Split(s, sep)
	for _, part := range splits {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}