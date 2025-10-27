package memory

import (
	"fmt"
	"sync"

	"simplebson/config"
	"simplebson/storage"
)

// Storage represents in-memory storage with persistent backup
type Storage struct {
	config     *config.Config
	store      *storage.Store
	records    map[string]map[string]interface{} // schema -> key -> record
	schemas    map[string]string                 // schema name -> field definitions
	mutex      sync.RWMutex
}

// NewStorage creates a new in-memory storage instance
func NewStorage(config *config.Config) *Storage {
	s := &Storage{
		config:  config,
		store:   storage.NewStore(config.StoragePath),
		records: make(map[string]map[string]interface{}),
		schemas: make(map[string]string),
	}

	// Load existing data from persistent storage
	s.loadFromPersistent()

	return s
}

// loadFromPersistent loads data from persistent storage
func (s *Storage) loadFromPersistent() {
	records, err := s.store.LoadRecords()
	if err != nil {
		// If loading fails, start with empty records
		s.records = make(map[string]map[string]interface{})
	} else {
		s.records = records
	}

	schemas, err := s.store.LoadSchemas()
	if err != nil {
		// If loading schemas fails, start with empty schemas
		s.schemas = make(map[string]string)
	} else {
		s.schemas = schemas
	}
}

// saveToPersistent saves data to persistent storage
func (s *Storage) saveToPersistent() error {
	if err := s.store.SaveRecords(s.records); err != nil {
		return err
	}
	
	if err := s.store.SaveSchemas(s.schemas); err != nil {
		return err
	}
	
	return nil
}

// CreateSchema creates a new schema
func (s *Storage) CreateSchema(name string, fields string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.schemas[name] = fields

	// Initialize the schema's record map if it doesn't exist
	if _, exists := s.records[name]; !exists {
		s.records[name] = make(map[string]interface{})
	}

	return s.saveToPersistent()
}

// GetSchema retrieves schema definition
func (s *Storage) GetSchema(name string) (string, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	schema, exists := s.schemas[name]
	if !exists {
		return "", fmt.Errorf("schema '%s' does not exist", name)
	}

	return schema, nil
}

// ListSchemas returns all defined schemas
func (s *Storage) ListSchemas() []string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	schemaNames := make([]string, 0, len(s.schemas))
	for name := range s.schemas {
		schemaNames = append(schemaNames, name)
	}

	return schemaNames
}

// AddRecord adds a record to a schema
func (s *Storage) AddRecord(schemaName string, recordData string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Check if schema exists
	_, exists := s.schemas[schemaName]
	if !exists {
		return fmt.Errorf("schema '%s' does not exist", schemaName)
	}

	// In a real implementation, we would parse the recordData properly
	// For now, we'll just store it as-is with a simple key
	// In the real implementation, we should extract the primary key from the record
	key := extractKeyFromRecord(recordData) // This is a placeholder function

	// Initialize the schema's record map if it doesn't exist
	if _, exists := s.records[schemaName]; !exists {
		s.records[schemaName] = make(map[string]interface{})
	}

	// Store the record
	s.records[schemaName][key] = recordData

	return s.saveToPersistent()
}

// GetRecord retrieves a record from a schema
func (s *Storage) GetRecord(schemaName string, key string) (interface{}, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Check if schema exists
	_, exists := s.schemas[schemaName]
	if !exists {
		return nil, fmt.Errorf("schema '%s' does not exist", schemaName)
	}

	// Check if record exists
	record, exists := s.records[schemaName][key]
	if !exists {
		return nil, fmt.Errorf("record with key '%s' does not exist in schema '%s'", key, schemaName)
	}

	return record, nil
}

// DeleteRecord removes a record from a schema
func (s *Storage) DeleteRecord(schemaName string, key string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Check if schema exists
	_, exists := s.schemas[schemaName]
	if !exists {
		return fmt.Errorf("schema '%s' does not exist", schemaName)
	}

	// Check if record exists
	_, exists = s.records[schemaName][key]
	if !exists {
		return fmt.Errorf("record with key '%s' does not exist in schema '%s'", key, schemaName)
	}

	// Delete the record
	delete(s.records[schemaName], key)

	return s.saveToPersistent()
}

// ListRecords returns all records of a schema
func (s *Storage) ListRecords(schemaName string) ([]interface{}, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Check if schema exists
	_, exists := s.schemas[schemaName]
	if !exists {
		return nil, fmt.Errorf("schema '%s' does not exist", schemaName)
	}

	records := make([]interface{}, 0)
	for _, record := range s.records[schemaName] {
		records = append(records, record)
	}

	return records, nil
}

// extractKeyFromRecord extracts key from record data (placeholder implementation)
func extractKeyFromRecord(recordData string) string {
	// This is a simplified implementation
	// In a real implementation, we would parse the JSON and extract the primary key field
	// For now, we'll just take the first word or a simple identifier
	// A proper implementation would parse the JSON and extract the name or id field
	
	// This is a very basic placeholder - in reality, we would properly parse the record
	// to extract the primary key field based on the schema
	return recordData
}