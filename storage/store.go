package storage

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

// Store handles persistent storage of records in BSON format
type Store struct {
	filePath string
}

// NewStore creates a new storage instance
func NewStore(filePath string) *Store {
	return &Store{
		filePath: filePath,
	}
}

// SaveRecords saves records to the storage file
func (s *Store) SaveRecords(records map[string]map[string]interface{}) error {
	// Create directory if it doesn't exist
	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	// Convert the records map to JSON and save
	jsonData, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal records: %v", err)
	}

	if err := ioutil.WriteFile(s.filePath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}

	return nil
}

// LoadRecords loads records from the storage file
func (s *Store) LoadRecords() (map[string]map[string]interface{}, error) {
	// Check if file exists
	if _, err := os.Stat(s.filePath); os.IsNotExist(err) {
		// Return empty records if file doesn't exist
		return make(map[string]map[string]interface{}), nil
	}

	data, err := ioutil.ReadFile(s.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %v", err)
	}

	var records map[string]map[string]interface{}
	if err := json.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("failed to unmarshal records: %v", err)
	}

	return records, nil
}

// SaveSchemas saves schema definitions to the storage file
func (s *Store) SaveSchemas(schemas map[string]string) error {
	// For simplicity, we'll save schemas in the same file as records
	// In a full implementation, schemas might be stored separately
	records, err := s.LoadRecords()
	if err != nil {
		return err
	}

	// Store schemas in a special "schemas" entry
	schemaJson, err := json.Marshal(schemas)
	if err != nil {
		return fmt.Errorf("failed to marshal schemas: %v", err)
	}

	records["schemas"] = map[string]interface{}{"definition": schemaJson}
	
	return s.SaveRecords(records)
}

// LoadSchemas loads schema definitions from the storage file
func (s *Store) LoadSchemas() (map[string]string, error) {
	records, err := s.LoadRecords()
	if err != nil {
		return nil, err
	}

	schemas := make(map[string]string)
	
	schemaData, exists := records["schemas"]
	if !exists {
		return schemas, nil
	}

	schemaJson, ok := schemaData["definition"]
	if !ok {
		return schemas, nil
	}
	
	schemaBytes, ok := schemaJson.(string)
	if !ok {
		// If it's already a byte array, handle that case
		jsonBytes, ok := schemaJson.([]byte)
		if !ok {
			return schemas, nil
		}
		schemaBytes = string(jsonBytes)
	}

	err = json.Unmarshal([]byte(schemaBytes), &schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal schemas: %v", err)
	}

	return schemas, nil
}