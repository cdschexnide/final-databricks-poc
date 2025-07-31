package blade

//   Purpose: Defines the configuration for each supported BLADE data type.

//   Fields:
//   - DataType: The BLADE data category identifier ("maintenance", "sortie", etc.)
//   - TableName: The corresponding Databricks table name where this data will be stored
//   - SourcePath: Mock path identifier for POC (uses "mock://" protocol)
//   - Description: Human-readable description of what this data type contains

type BLADEDataMapping struct {
	DataType    string `json:"dataType"` // BLADE data type
	TableName   string `json:"tableName"` // corresponding Databricks table name
	SourcePath  string `json:"sourcePath"` // mock source path for POC (not a real data path)
	Description string `json:"description"`
}

//   Purpose: Returns the complete set of supported BLADE data type configurations.
func GetBLADEMappings() []BLADEDataMapping {
	return []BLADEDataMapping{
		// - Data Type: Aircraft maintenance records
		// - Table: blade_maintenance_data in Databricks
		// - Content: Scheduled/unscheduled maintenance, parts, labor hours, technician assignments
		{
			DataType:    "maintenance",
			TableName:   "blade_maintenance_data",
			SourcePath:  "mock://maintenance",
			Description: "Aircraft maintenance schedules and predictive maintenance data",
		},
		// - Data Type: Flight operations and mission data
		// - Table: blade_sortie_schedules in Databricks
		// - Content: Training missions, combat operations, pilot assignments, aircraft configurations
		{
			DataType:    "sortie",
			TableName:   "blade_sortie_schedules",
			SourcePath:  "mock://sortie", 
			Description: "Flight schedules and sortie planning data",
		},
		// - Data Type: Personnel and equipment deployment operations
		// - Table: blade_deployment_plans in Databricks
		// - Content: Squadron rotations, equipment movements, deployment timelines, personnel manifests
		{
			DataType:    "deployment",
			TableName:   "blade_deployment_plans",
			SourcePath:  "mock://deployment", 
			Description: "Deployment preparation and logistics planning",
		},
		// - Data Type: Supply chain and logistics operations
		// - Table: blade_logistics_general in Databricks
		// - Content: Supply requests, fuel management, munitions, equipment transfers, HAZMAT shipments
		{
			DataType:    "logistics",
			TableName:   "blade_logistics_general",
			SourcePath:  "mock://logistics",
			Description: "General logistics and supply chain data",
		},
	}
}
