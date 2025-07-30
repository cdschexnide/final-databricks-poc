# Mock BLADE Data Files

This directory contains realistic mock data representing the BLADE (Basing & Logistics Analytics Data Environment) system used by the U.S. Air Force. All data is **UNCLASSIFIED** and created for proof-of-concept demonstration purposes.

## Data Categories

### 1. **maintenance_data.json** - Aircraft Maintenance Records
Contains detailed maintenance activities including:
- **Scheduled Maintenance**: Regular inspections (100-hour, 500-hour, phase maintenance)
- **Unscheduled Repairs**: Issue-driven maintenance (avionics problems, system failures)
- **Specialized Maintenance**: Low observable coating (F-22), weapons systems (A-10)
- **Key Fields**:
  - Aircraft identification (tail number, type)
  - Maintenance codes and compliance references
  - Parts requirements and costs
  - Labor hours and technician assignments
  - Safety notes and work orders

**Sample Aircraft Types**: F-16C/D, A-10C, F-22A

### 2. **logistics_data.json** - Supply Chain & Logistics
Comprehensive logistics operations including:
- **Supply Requests**: Aircraft parts, consumables
- **Fuel Management**: JP-8 fuel requests and allocations
- **Munitions Movement**: Training ordnance handling
- **Equipment Transfers**: AGE (Aerospace Ground Equipment) redistribution
- **HAZMAT Shipments**: Dangerous goods transportation
- **Key Fields**:
  - NSN (National Stock Numbers)
  - Fund citations and project codes
  - Transportation tracking
  - Approval workflows

### 3. **sortie_data.json** - Flight Operations
Detailed flight mission data covering:
- **Training Missions**: Air combat training, BFM/ACM
- **Close Air Support**: A-10 CAS training operations
- **Air Sovereignty**: F-22 homeland defense alerts
- **Large Force Exercises**: RED FLAG operations
- **Search and Rescue**: Combat SAR training
- **Key Fields**:
  - Mission planning details
  - Aircraft configurations and loadouts
  - Pilot information and flight hours
  - Airspace coordination
  - Mission effectiveness metrics

### 4. **deployment_data.json** - Deployment Operations
Complete deployment lifecycle data:
- **Personnel Deployments**: Squadron rotations to forward bases
- **Equipment Deployments**: Patriot battery movements
- **Humanitarian Operations**: PACIFIC ANGEL missions
- **Exercise Deployments**: Multinational training exercises
- **Emergency Response**: Natural disaster relief operations
- **Key Fields**:
  - Deployment orders and timelines
  - Personnel manifests
  - Equipment lists
  - Transportation requirements
  - Command structure

## Data Structure

All files follow consistent patterns:
- **item_id**: Unique identifier following military naming conventions
- **item_type**: Category within the data type
- **classification_marking**: Always "UNCLASSIFIED" for this POC
- **timestamp**: ISO 8601 format timestamps
- **Additional fields**: Specific to each data category

## Classification Note

All data in these files is **UNCLASSIFIED** and created for demonstration purposes. Real BLADE data would include various classification levels and require appropriate handling.

## Usage Example

```json
{
  "item_id": "F16-001-ENG-2024",
  "item_type": "engine_maintenance",
  "classification_marking": "UNCLASSIFIED",
  "timestamp": "2024-01-15T10:30:00Z",
  "aircraft_tail": "87-0294",
  "aircraft_type": "F-16C",
  "maintenance_type": "scheduled",
  ...
}
```

## Military Terminology

- **NSN**: National Stock Number (supply system identifier)
- **DODIC**: Department of Defense Identification Code (ammunition)
- **DSN**: Defense Switched Network (military phone system)
- **TO**: Technical Order (maintenance manual reference)
- **AFI**: Air Force Instruction (regulation)
- **AGE**: Aerospace Ground Equipment
- **EMEDS**: Expeditionary Medical Support
- **ROWPU**: Reverse Osmosis Water Purification Unit
- **BFM/ACM**: Basic/Advanced Fighter Maneuvers, Air Combat Maneuvering

## Base Locations

Mock data references real U.S. Air Force bases:
- **Nellis AFB**: Nevada (home of RED FLAG exercises)
- **Davis-Monthan AFB**: Arizona (A-10 operations)
- **Holloman AFB**: New Mexico (F-16 training)
- **Langley AFB**: Virginia (F-22 operations)
- **Kadena AB**: Japan (Pacific operations)
- **Al Udeid AB**: Qatar (CENTCOM operations)

## Data Volume

- **Maintenance**: 5 detailed records covering different aircraft and maintenance types
- **Logistics**: 5 records spanning supply, fuel, munitions, and hazmat
- **Sortie**: 5 flight operations from training to combat exercises
- **Deployment**: 5 deployment scenarios from routine to emergency

This represents a small sample of what BLADE would contain - the real system manages data for the entire Air Force enterprise.