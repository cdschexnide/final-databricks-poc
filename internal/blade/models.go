package blade

type BLADEDataMapping struct {
	DataType    string `json:"dataType"` // BLADE data type
	TableName   string `json:"tableName"` // corresponding Databricks table name
	SourcePath  string `json:"sourcePath"` // mock path for POC (not real files)
	Description string `json:"description"`
}

// func => return the standard BLADE data type mappings
// uses mock source paths
func GetBLADEMappings() []BLADEDataMapping {
	return []BLADEDataMapping{
		{
			DataType:    "maintenance",
			TableName:   "blade_maintenance_data",
			SourcePath:  "mock://maintenance",
			Description: "Aircraft maintenance schedules and predictive maintenance data",
		},
		{
			DataType:    "sortie",
			TableName:   "blade_sortie_schedules",
			SourcePath:  "mock://sortie", 
			Description: "Flight schedules and sortie planning data",
		},
		{
			DataType:    "deployment",
			TableName:   "blade_deployment_plans",
			SourcePath:  "mock://deployment", 
			Description: "Deployment preparation and logistics planning",
		},
		{
			DataType:    "logistics",
			TableName:   "blade_logistics_general",
			SourcePath:  "mock://logistics",
			Description: "General logistics and supply chain data",
		},
	}
}
