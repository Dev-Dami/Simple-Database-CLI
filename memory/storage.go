package memory

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"simplebson/config"
	"simplebson/storage"
)

// DatabaseState holds the data for a single database
type DatabaseState struct {
	records     map[string]map[string]interface{} // Maps schemas to records
	schemas     map[string]string                 // Schema definitions
	partialKeys map[string]map[string][]string    // For partial key lookups
}

// Storage manages records in memory with BSON persistence
type Storage struct {
	config    *config.Config
	stores    map[string]*storage.Store // Maps database names to stores
	dbStates  map[string]*DatabaseState // Maps database names to their data state
	currentDB string                    // The currently selected database
	mutex     sync.RWMutex
}

// NewStorage creates a new storage instance with persistence
func NewStorage(config *config.Config) *Storage {
	s := &Storage{
		config:    config,
		stores:    make(map[string]*storage.Store),
		dbStates:  make(map[string]*DatabaseState),
		currentDB: "default", // Default database
	}

	// Initialize default database state
	s.dbStates["default"] = &DatabaseState{
		records:     make(map[string]map[string]interface{}),
		schemas:     make(map[string]string),
		partialKeys: make(map[string]map[string][]string),
	}

	// Load existing data from persistent storage for default database
	s.loadFromPersistent()

	return s
}

// getOrCreateStore returns the store for the given database, creating it if it doesn't exist
func (s *Storage) getOrCreateStore(dbName string) *storage.Store {
	if store, exists := s.stores[dbName]; exists {
		return store
	}

	// If the store doesn't exist, create a new one
	dbPath := filepath.Join(filepath.Dir(s.config.StoragePath), dbName)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		// Handle error, maybe log it or return an error
	}
	storagePath := filepath.Join(dbPath, "store.bson")
	newStore := storage.NewStore(storagePath)
	s.stores[dbName] = newStore
	return newStore
}

// getDBState returns the state for the given database, creating it if it doesn't exist
func (s *Storage) getDBState(dbName string) *DatabaseState {
	if dbState, exists := s.dbStates[dbName]; exists {
		return dbState
	}

	// Create new database state
	dbState := &DatabaseState{
		records:     make(map[string]map[string]interface{}),
		schemas:     make(map[string]string),
		partialKeys: make(map[string]map[string][]string),
	}
	s.dbStates[dbName] = dbState
	return dbState
}

// loadFromPersistent loads data from the BSON file for the current database
func (s *Storage) loadFromPersistent() {
	store := s.getOrCreateStore(s.currentDB)
	dbState := s.getDBState(s.currentDB)
	
	records, err := store.LoadRecords()
	if err != nil {
		dbState.records = make(map[string]map[string]interface{})
	} else {
		dbState.records = records
	}

	schemas, err := store.LoadSchemas()
	if err != nil {
		dbState.schemas = make(map[string]string)
	} else {
		dbState.schemas = schemas
	}

	s.rebuildPartialKeyIndex()
}

// rebuildPartialKeyIndex builds partial key lookup table for current database
func (s *Storage) rebuildPartialKeyIndex() {
	dbState := s.getDBState(s.currentDB)
	dbState.partialKeys = make(map[string]map[string][]string)

	for schemaName, schemaRecords := range dbState.records {
		if schemaName == "__schemas__" {
			continue
		}

		dbState.partialKeys[schemaName] = make(map[string][]string)

		for fullKey := range schemaRecords {
			partialKey := getPartialKey(fullKey)
			if _, exists := dbState.partialKeys[schemaName][partialKey]; !exists {
				dbState.partialKeys[schemaName][partialKey] = []string{}
			}
			dbState.partialKeys[schemaName][partialKey] = append(dbState.partialKeys[schemaName][partialKey], fullKey)
		}
	}
}

// saveToPersistent writes data to the BSON file for the current database
func (s *Storage) saveToPersistent() error {
	store := s.getOrCreateStore(s.currentDB)
	dbState := s.getDBState(s.currentDB)
	
	if err := store.SaveRecords(dbState.records); err != nil {
		return err
	}

	if err := store.SaveSchemas(dbState.schemas); err != nil {
		return err
	}

	return nil
}

// UseDB switches to a different database
func (s *Storage) UseDB(dbName string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Save current database data before switching
	s.saveToPersistent()

	// Switch to new database
	s.currentDB = dbName
	s.loadFromPersistent()
}

// ListDBs lists all available databases
func (s *Storage) ListDBs() ([]string, error) {
	files, err := ioutil.ReadDir(filepath.Dir(s.config.StoragePath))
	if err != nil {
		return nil, fmt.Errorf("failed to read storage directory: %v", err)
	}

	var dbs []string
	for _, file := range files {
		if file.IsDir() {
			dbs = append(dbs, file.Name())
		}
	}

	return dbs, nil
}

// CreateSchema adds a new schema definition
func (s *Storage) CreateSchema(name string, fields string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	dbState := s.getDBState(s.currentDB)
	dbState.schemas[name] = fields

	if _, exists := dbState.records[name]; !exists {
		dbState.records[name] = make(map[string]interface{})
	}

	return s.saveToPersistent()
}

// GetSchema returns a schema definition
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

	if _, exists := s.schemas[schemaName]; !exists {
		return fmt.Errorf("schema '%s' does not exist", schemaName)
	}

	// Parse the incoming record
	var parsedRecord map[string]interface{}
	if err := json.Unmarshal([]byte(recordData), &parsedRecord); err != nil {
		return fmt.Errorf("invalid JSON format: %v", err)
	}

	// Add timestamp fields
	now := time.Now().Format(time.RFC3339)
	parsedRecord["created_at"] = now
	parsedRecord["updated_at"] = now

	// Marshal back to JSON string
	updatedRecordData, err := json.Marshal(parsedRecord)
	if err != nil {
		return fmt.Errorf("failed to marshal updated record: %v", err)
	}

	// Validate the record with the new timestamp fields
	if err := s.validateRecordAgainstSchema(schemaName, string(updatedRecordData)); err != nil {
		return fmt.Errorf("record validation failed: %v", err)
	}

	key := extractKeyFromRecord(string(updatedRecordData))
	if key == "" || key == string(updatedRecordData) {
		if err := json.Unmarshal(updatedRecordData, &parsedRecord); err == nil {
			for _, field := range []string{"id", "name", "key"} {
				if val, exists := parsedRecord[field]; exists {
					key = fmt.Sprintf("%v", val)
					break
				}
			}
		}
	}

	if key == "" {
		return fmt.Errorf("could not extract a valid key from record data: %s", string(updatedRecordData))
	}

	if _, exists := s.records[schemaName]; !exists {
		s.records[schemaName] = make(map[string]interface{})
	}

	s.records[schemaName][key] = string(updatedRecordData)
	s.updatePartialKeyIndex(schemaName, key, true)

	return s.saveToPersistent()
}

// validateRecordAgainstSchema checks if record matches schema types
func (s *Storage) validateRecordAgainstSchema(schemaName string, recordData string) error {
	schemaDef, exists := s.schemas[schemaName]
	if !exists {
		return fmt.Errorf("schema '%s' does not exist", schemaName)
	}

	var record map[string]interface{}
	if err := json.Unmarshal([]byte(recordData), &record); err != nil {
		return fmt.Errorf("invalid JSON format: %v", err)
	}

	fields := parseSchemaFields(schemaDef)

	for field, fieldType := range fields {
		if _, exists := record[field]; !exists {
			continue
		}

		if err := validateFieldType(record[field], fieldType); err != nil {
			return fmt.Errorf("field '%s' type validation failed: %v", field, err)
		}
	}

	return nil
}

// parseSchemaFields parses the schema definition string and returns fields and their types
func parseSchemaFields(schemaDef string) map[string]string {
	fields := make(map[string]string)
	parts := strings.Split(schemaDef, " ")

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// Split by colon to separate field name and type (e.g., "name:string")
		pair := strings.Split(part, ":")
		if len(pair) == 2 {
			fieldName := strings.TrimSpace(pair[0])
			fieldType := strings.TrimSpace(pair[1])
			fields[fieldName] = fieldType
		}
	}

	return fields
}

// validateFieldType checks if value matches expected type
func validateFieldType(value interface{}, expectedType string) error {
	switch expectedType {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("expected string, got %T", value)
		}
	case "int", "integer":
		// JSON unmarshaling may represent numbers as float64
		switch v := value.(type) {
		case float64:
			// Check if it's a whole number
			if v != float64(int64(v)) {
				return fmt.Errorf("expected integer, got float: %v", value)
			}
		case int, int32, int64:
			// These are valid integer types
		default:
			return fmt.Errorf("expected integer, got %T", value)
		}
	case "float", "double":
		_, ok := value.(float64)
		if !ok {
			return fmt.Errorf("expected float, got %T", value)
		}
	case "bool", "boolean":
		_, ok := value.(bool)
		if !ok {
			return fmt.Errorf("expected bool, got %T", value)
		}
	case "object", "json":
		// Accept any type for object/json type
		return nil
	default:
		// For unknown types, accept any value for MVP
		return nil
	}

	return nil
}

// getPartialKey returns the first 5 characters of the key as the partial key
func getPartialKey(fullKey string) string {
	if len(fullKey) <= 5 {
		return fullKey
	}
	return fullKey[:5]
}

// updatePartialKeyIndex adds or removes a key from the partial key index
func (s *Storage) updatePartialKeyIndex(schemaName, fullKey string, add bool) {
	if _, exists := s.partialKeys[schemaName]; !exists {
		s.partialKeys[schemaName] = make(map[string][]string)
	}

	partialKey := getPartialKey(fullKey)

	if add {
		// Add the full key to the partial key list if not already there
		found := false
		for _, key := range s.partialKeys[schemaName][partialKey] {
			if key == fullKey {
				found = true
				break
			}
		}
		if !found {
			s.partialKeys[schemaName][partialKey] = append(s.partialKeys[schemaName][partialKey], fullKey)
		}
	} else {
		// Remove the full key from the partial key list
		newKeys := []string{}
		for _, key := range s.partialKeys[schemaName][partialKey] {
			if key != fullKey {
				newKeys = append(newKeys, key)
			}
		}
		s.partialKeys[schemaName][partialKey] = newKeys
	}
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

	// First, try exact key match
	record, exists := s.records[schemaName][key]
	if exists {
		return record, nil
	}

	// If exact match not found, try partial key lookup
	partialMatches := s.getRecordsByPartialKey(schemaName, key)
	if len(partialMatches) == 1 {
		// If there's exactly one match with the partial key, return it
		fullKey := partialMatches[0]
		record, exists := s.records[schemaName][fullKey]
		if exists {
			return record, nil
		}
	} else if len(partialMatches) > 1 {
		// If multiple matches, return an error indicating ambiguity
		return nil, fmt.Errorf("multiple records match partial key '%s' in schema '%s': %v", key, schemaName, partialMatches)
	}

	// No matches found
	return nil, fmt.Errorf("record with key '%s' does not exist in schema '%s'", key, schemaName)
}

// getRecordsByPartialKey returns the list of full keys that match the partial key
func (s *Storage) getRecordsByPartialKey(schemaName string, partialKey string) []string {
	if partialKey == "" {
		return []string{}
	}

	var matches []string

	// If the partial key is at least 5 characters, look it up directly
	if len(partialKey) >= 5 {
		lookupKey := partialKey[:5]
		if schemaIndex, exists := s.partialKeys[schemaName]; exists {
			if keys, exists := schemaIndex[lookupKey]; exists {
				// Filter keys that actually start with the partial key
				for _, key := range keys {
					if strings.HasPrefix(key, partialKey) {
						matches = append(matches, key)
					}
				}
			}
		}
	} else {
		// If the partial key is less than 5 characters,
		// we need to look for any partial key entries that start with this prefix
		if schemaIndex, exists := s.partialKeys[schemaName]; exists {
			for partial, keys := range schemaIndex {
				if strings.HasPrefix(partial, partialKey) || strings.HasPrefix(partialKey, partial) {
					// Check if any of the keys in this partial match start with the partialKey
					for _, key := range keys {
						if strings.HasPrefix(key, partialKey) {
							matches = append(matches, key)
						}
					}
				}
			}
		}
	}

	return matches
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

	// Update partial key index
	s.updatePartialKeyIndex(schemaName, key, false)

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

// WipeDatabase clears all records and schemas from the database
func (s *Storage) WipeDatabase() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Clear all data structures
	s.records = make(map[string]map[string]interface{})
	s.schemas = make(map[string]string)
	s.partialKeys = make(map[string]map[string][]string)

	// Save the empty state to persistent storage
	return s.saveToPersistent()
}

// extractKeyFromRecord extracts key from record data by looking for common key fields
func extractKeyFromRecord(recordData string) string {
	var record map[string]interface{}

	// Try to parse the record data as JSON
	if err := json.Unmarshal([]byte(recordData), &record); err != nil {
		// If JSON parsing fails, return a default key
		return recordData
	}

	// Look for common key fields in order of preference
	keyFields := []string{"id", "name", "key"}

	for _, field := range keyFields {
		if value, exists := record[field]; exists {
			if strValue, ok := value.(string); ok {
				return strValue
			}
			// Convert non-string values to string
			return fmt.Sprintf("%v", value)
		}
	}

	// If no common key fields found, try to use the first field as key
	for key, value := range record {
		if strValue, ok := value.(string); ok {
			return strValue
		}
		// If the value is not a string, return the key name as the identifier
		return key
	}

	// Fallback to the original record data
	return recordData
}
