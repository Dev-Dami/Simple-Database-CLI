package preprocessing

import (
	"fmt"
)

// ParseCommand parses command-line arguments for different commands
func ParseCommand(command string, args []string) ([]string, error) {
	switch command {
	case "add":
		// Format: add <schema> <record_data>
		if len(args) < 2 {
			return nil, fmt.Errorf("not enough arguments for 'add' command")
		}
		return args, nil

	case "get", "view", "delete":
		// Format: get/view/delete <schema> <key>
		if len(args) < 2 {
			return nil, fmt.Errorf("not enough arguments for '%s' command", command)
		}
		return args, nil

	case "list":
		// Format: list <schema>
		if len(args) < 1 {
			return nil, fmt.Errorf("not enough arguments for 'list' command")
		}
		return args, nil

	case "schema":
		// Format: schema <schema_name> [field_definitions...]
		// If no args provided, this is to list all schemas
		return args, nil

	case "wipe", "drop":
		// Format: wipe/drop (no args needed)
		return args, nil

	default:
		return nil, fmt.Errorf("unknown command: %s", command)
	}
}

// ExtractSchemaName extracts the schema name from a record string
// This is a simplified implementation - in a real implementation, 
// this would parse the JSON-like format properly
func ExtractSchemaName(recordData string) (string, error) {
	// This is a simplified version - in the real implementation, 
	// we would parse the record data to extract the primary key
	// For now, we'll just return the first value found in the record
	return recordData, nil
}