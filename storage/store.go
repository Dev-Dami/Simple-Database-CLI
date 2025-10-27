package storage

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	
	"go.mongodb.org/mongo-driver/bson"
)

// Store handles persistent storage
type Store struct {
	filePath string
}

func NewStore(filePath string) *Store {
	return &Store{
		filePath: filePath,
	}
}

func (s *Store) SaveRecords(records map[string]map[string]interface{}) error {
	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %v", err)
	}

	bsonData, err := bson.Marshal(records)
	if err != nil {
		return fmt.Errorf("failed to marshal records: %v", err)
	}

	if err := ioutil.WriteFile(s.filePath, bsonData, 0644); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}

	return nil
}

func (s *Store) LoadRecords() (map[string]map[string]interface{}, error) {
	if _, err := os.Stat(s.filePath); os.IsNotExist(err) {
		return make(map[string]map[string]interface{}), nil
	}

	data, err := ioutil.ReadFile(s.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %v", err)
	}

	var records map[string]map[string]interface{}
	if err := bson.Unmarshal(data, &records); err != nil {
		return nil, fmt.Errorf("failed to unmarshal records: %v", err)
	}

	return records, nil
}

// SaveSchemas saves schema definitions to the storage file
func (s *Store) SaveSchemas(schemas map[string]string) error {
	records, err := s.LoadRecords()
	if err != nil {
		return err
	}

	// Store schemas in a special "__schemas__" entry to avoid conflicts with actual records
	// For BSON, we can store the schemas directly in the structure
	// Ensure records map exists
	if records == nil {
		records = make(map[string]map[string]interface{})
	}
	records["__schemas__"] = make(map[string]interface{})
	
	// Iterate through the schema definitions and add them to the __schemas__ map
	for key, value := range schemas {
		records["__schemas__"][key] = value
	}

	return s.SaveRecords(records)
}

// LoadSchemas loads schema definitions from the storage file
func (s *Store) LoadSchemas() (map[string]string, error) {
	records, err := s.LoadRecords()
	if err != nil {
		return nil, err
	}

	schemas := make(map[string]string)

	schemaData, exists := records["__schemas__"]
	if !exists {
		return schemas, nil
	}

	// Iterate through the schema entries and convert them back to strings
	for key, value := range schemaData {
		if strValue, ok := value.(string); ok {
			schemas[key] = strValue
		}
	}

	return schemas, nil
}
