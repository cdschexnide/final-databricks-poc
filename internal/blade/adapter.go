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
	// - Creates empty map to store data type configurations
	// - Key: string (data type like "maintenance")
	// - Value: BLADEDataMapping struct with table name, source path, description
	mappings := make(map[string]BLADEDataMapping)

	// Index by DataType for fast lookup:
	// - mappings["maintenance"] → maintenance mapping
	// - mappings["sortie"] → sortie mapping
	// - mappings["deployment"] → deployment mapping
	// - mappings["logistics"] → logistics mapping
	for _, mapping := range GetBLADEMappings() {
		mappings[mapping.DataType] = mapping
	}

	// - dataSource: "BLADE_LOGISTICS" (from config)
	// - basePath: "mock_blade_data/" (from config)
	// - mappings: Index of all 4 supported data types
	return &BLADEAdapter{
		dataSource: dataSource,
		basePath:   basePath,
		mappings:   mappings,
	}
}

// this function serves as the bridge between BLADE data types/formats and Databricks ingestion requirements
func (b *BLADEAdapter) PrepareIngestionRequest(dataType string, format string) (*databricks.IngestionRequest, error) {
	// - Looks up dataType in the pre-indexed mappings from NewBLADEAdapter
	// - Fast O(1) lookup - no iteration needed
	// - Returns error immediately for invalid types like "invalid_type"
	// - mapping contains: TableName, SourcePath, Description for this data type
	mapping, exists := b.mappings[dataType]

	if !exists {
		return nil, fmt.Errorf("Unsupported BLADE data type: %s", dataType)
	}

	// - Sets default format to "JSON" if not specified
  	// - Handles cases where CLI omits format argument
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
	// - Creates empty string slice with zero length but capacity = len(b.mappings)
	// - Pre-allocates memory for exactly the right number of elements (4 in current implementation)
	// - Performance optimization - avoids slice growth/reallocation during appends
	types := make([]string, 0, len(b.mappings))

	// - Iterates over the mappings map using range on keys only
	// - dataType gets each key ("maintenance", "sortie", "deployment", "logistics")
	// - Appends each data type name to the types slice
	// - Note: Map iteration order is not guaranteed in Go
	for dataType := range b.mappings {
		types = append(types, dataType)
	}

	// - Returns the populated slice of supported data type names
	return types
}

func (b *BLADEAdapter) loadMockDataFile(dataType string) (string, error) {
	// - Uses string formatting to build standardized file names
  	// - Pattern: {dataType}_data.json
  	// - Examples:
    // 	- dataType = "maintenance" → fileName = "maintenance_data.json"
    // 	- dataType = "sortie" → fileName = "sortie_data.json"
    // 	- dataType = "deployment" → fileName = "deployment_data.json"
    // 	- dataType = "logistics" → fileName = "logistics_data.json"
	fileName := fmt.Sprintf("%s_data.json", dataType)

	// - Uses filepath.Join() for cross-platform path construction
  	// - Path Structure: {basePath}/{dataType}/{fileName}
  	// - Examples with b.basePath = "mock_blade_data/":
    // 	- "mock_blade_data/maintenance/maintenance_data.json"
    // 	- "mock_blade_data/sortie/sortie_data.json"
    // 	- "mock_blade_data/deployment/deployment_data.json"
    // 	- "mock_blade_data/logistics/logistics_data.json"
	filePath := filepath.Join(b.basePath, dataType, fileName)
	
	// - Uses ioutil.ReadFile() to read entire file into memory as []byte
  	// - Handles common file errors:
    // 	- File doesn't exist: no such file or directory
    // 	- Permission denied: permission denied
    // 	- Directory instead of file: is a directory
  	// - Error wrapping: Preserves original error with context about which file failed
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read mock data file %s: %w", filePath, err)
	}
	
	// - Converts []byte to string for JSON processing
  	// - Returns the raw JSON content exactly as stored in the file
	return string(data), nil
}

func (b *BLADEAdapter) loadMockCSVAsJSON(dataType string) (string, error) {
	// - Builds CSV file name: {dataType}_data.csv
	// - Constructs full path: mock_blade_data/maintenance/maintenance_data.csv
	// - Same pattern as loadMockDataFile but targets .csv files
	fileName := fmt.Sprintf("%s_data.csv", dataType)
	filePath := filepath.Join(b.basePath, dataType, fileName)
	
	// - Opens file for reading (not loading entire file into memory)
	// - Uses defer to ensure file is closed even if function exits early
	// - Error handling for missing files, permissions, etc.
	file, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to open CSV file %s: %w", filePath, err)
	}
	defer file.Close()
	
	// - Creates Go's standard CSV reader
  	// - Handles CSV parsing, quote escaping, field separation automatically
	reader := csv.NewReader(file)

	// - ReadAll() parses entire CSV to [][]string (array of rows, each row is array of fields)
	// - Validates CSV has at least 2 rows (headers + at least 1 data row)
	// - Structure: records[0] = headers, records[1+] = data rows
	records, err := reader.ReadAll()
	if err != nil {
		return "", fmt.Errorf("failed to read CSV file %s: %w", filePath, err)
	}
	if len(records) < 2 {
		return "", fmt.Errorf("CSV file %s has no data rows", filePath)
	}

	// - First row contains column names
  	// - Example: ["item_id", "item_type", "classification_marking", "timestamp", "parts_required", ...]
	headers := records[0]
	
	var jsonRecords []map[string]interface{}
	
	// 	 Row-by-Row Processing:
	// 	 - Skips header row (starts at i = 1)
	// 	 - Creates map[string]interface{} for each data row
	// 	 - Maps CSV columns to JSON fields using headers as keys

	//   Special Field Handling:
	//   - Array Fields (parts_required, compliance_refs):
	//     - CSV: "engine_oil_filter;spark_plugs;hydraulic_fluid"
	//     - JSON: ["engine_oil_filter", "spark_plugs", "hydraulic_fluid"]
	//     - Uses splitAndTrim() helper to split on semicolon and clean whitespace
	//   - Empty Values: Convert "" to null in JSON
	//   - Regular Values: Keep as strings
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
	
	// - Marshals []map[string]interface{} to JSON string
  	// - Returns JSON that matches the structure of native JSON files
	jsonData, err := json.Marshal(jsonRecords)
	if err != nil {
		return "", fmt.Errorf("failed to convert CSV to JSON: %w", err)
	}
	
	return string(jsonData), nil
}

func splitAndTrim(s string, sep string) []string {
	// - Splits string on separator (;)
	// - Trims whitespace from each part
	// - Filters out empty strings
	// - Example: "part1; part2 ; ; part3" → ["part1", "part2", "part3"]
	parts := []string{}
	splits := strings.Split(s, sep)
	for _, part := range splits {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			parts = append(parts, trimmed)
		}
	}
	return parts
}