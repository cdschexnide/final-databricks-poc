# BLADE Databricks PoC

A Go application that ingests BLADE logistics data into Databricks. Supports data in both JSON and CSV formats.

## Setup

### Prerequisites
- Go 1.24.5 or later
- Databricks workspace with SQL warehouse
- Personal access token for Databricks

### Install Dependencies
```bash
go mod download
```

### Environment Configuration
Create a `.env` file in the project root:

```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
DATABRICKS_WAREHOUSE_ID=your-warehouse-id
DATABRICKS_CATALOG=blade_poc
DATABRICKS_SCHEMA=logistics
```

## Usage

### Basic Commands
```bash
# For PoC, the default option runs data ingestion with maintenance data in JSON format
go run cmd/main.go

# Specific data type
go run cmd/main.go sortie

# Specific data type and file format
go run cmd/main.go logistics CSV
```

### Mock BLADE Data Types
- `maintenance` - Aircraft maintenance records
- `sortie` - Flight operations and missions  
- `deployment` - Personnel and equipment deployments
- `logistics` - Supply chain and logistics data

### Supported File Formats
- `JSON` - Native JSON files
- `CSV` - CSV files (converted to JSON internally)

## Testing

### Run All Tests
```bash
go test -v
```

### Run Unit Tests Only
```bash
go test -v -run TestBLADEAdapterMappings
```

### Run Performance Benchmarks
```bash
go test -bench=BenchmarkBLADEIngestion
```

**Note:** Integration tests require Databricks credentials

## Project Structure
```
cmd/main.go              # CLI entry point
internal/
 blade/               # BLADE data processing
 config/              # Environment configuration  
 databricks/          # Databricks client and operations
mock_blade_data/         # Sample data files
integration_test.go      # End-to-end tests
```