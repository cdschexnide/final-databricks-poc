package blade

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"databricks-blade-poc/internal/databricks"
)

type BLADEAdapter struct {
	dataSource string // a specific BLADE deployment
	basePath string // the root volume path where BLADE stores data files
	mappings map[string]BLADEDataMapping // map of data type -> table configuration (for quick lookup)
}

// returns pointer to a configured BLADEAdapter
func NewBLADEAdapter(dataSource, basePath string) *BLADEAdapter {
	mappings := make(map[string]BLADEDataMapping)

	// loads all available BLADE mappings, indexing by BLADE data type
	for _, mapping := range GetBLADEMappings() {
		mappings[mapping.DataType] = mapping
	}

	return &BLADEAdapter{
		dataSource: dataSource,
		basePath:   basePath,
		mappings:   mappings,
	}
}

// takes in a data type
// returns a properly formatted IngestionRequest
func (b *BLADEAdapter) PrepareIngestionRequest(dataType string) (*databricks.IngestionRequest, error) {
	// Look up the mapping for this data type
	mapping, exists := b.mappings[dataType]

	if !exists {
		return nil, fmt.Errorf("Unsupported BLADE data type: %s", dataType)
	}

	// Load JSON data from file
	sampleData, err := b.loadMockDataFile(dataType)
	if err != nil {
		return nil, fmt.Errorf("failed to load mock data for %s: %w", dataType, err)
	}

	return &databricks.IngestionRequest{
		TableName:     mapping.TableName,
		SourcePath:    "mock://" + dataType, // Mock path for POC
		FileFormat:    "JSON",
		FormatOptions: "'multiLine' = 'true', 'inferSchema' = 'true'",
		DataSource:    b.dataSource,
		SampleData:    sampleData, // Add the loaded mock data
		Metadata: map[string]string{
			"source_system": "BLADE",
			"data_type":     dataType,
			"integration":   "databricks_poc",
			"description":   mapping.Description,
			"mode":          "mock_data",
		},
	}, nil
}

// request validation
func (b *BLADEAdapter) GetSupportedDataTypes() []string {
	types := make([]string, 0, len(b.mappings))
	for dataType := range b.mappings {
		types = append(types, dataType)
	}
	return types
}

// loads JSON data from the mock_blade_data directory
func (b *BLADEAdapter) loadMockDataFile(dataType string) (string, error) {
	fileName := fmt.Sprintf("%s_data.json", dataType)
	filePath := filepath.Join(b.basePath, dataType, fileName)
	
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read mock data file %s: %w", filePath, err)
	}
	
	return string(data), nil
}