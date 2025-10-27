package memory

import (
	"encoding/json"
	"fmt"
	"strings"
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
	partialKeys map[string]map[string][]string   // schema -> partial key -> list of full keys
	mutex      sync.RWMutex
}

// NewStorage creates a new in-memory storage instance
func NewStorage(config *config.Config) *Storage {
	s := &Storage{
		config:      config,
		store:       storage.NewStore(config.StoragePath),
		records:     make(map[string]map[string]interface{}),
		schemas:     make(map[string]string),
		partialKeys: make(map[string]map[string][]string), // Initialize partial keys map
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
	
	// Rebuild partial key index after loading data
	s.rebuildPartialKeyIndex()
}

// rebuildPartialKeyIndex rebuilds the partial key index from current records
func (s *Storage) rebuildPartialKeyIndex() {
	s.partialKeys = make(map[string]map[string][]string)
	
	for schemaName, schemaRecords := range s.records {
		if schemaName == "__schemas__" { // Skip the special schemas entry
			continue
		}
		
		s.partialKeys[schemaName] = make(map[string][]string)
		
		for fullKey := range schemaRecords {
			partialKey := getPartialKey(fullKey)
			if _, exists := s.partialKeys[schemaName][partialKey]; !exists {
				s.partialKeys[schemaName][partialKey] = []string{}
			}
			s.partialKeys[schemaName][partialKey] = append(s.partialKeys[schemaName][partialKey], fullKey)
		}
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

	// Validate record against schema
	err := s.validateRecordAgainstSchema(schemaName, recordData)
	if err != nil {
		return fmt.Errorf("record validation failed: %v", err)
	}

	// Extract the key from the record data
	key := extractKeyFromRecord(recordData)
	if key == "" || key == recordData {
		// If we couldn't extract a proper key, try to parse the record to get a meaningful key
		parsedRecord := make(map[string]interface{})
		if err := json.Unmarshal([]byte(recordData), &parsedRecord); err == nil {
			// Use the original recordData as a fallback key if needed
			for _, field := range []string{"id", "name", "key"} {
				if val, exists := parsedRecord[field]; exists {
					key = fmt.Sprintf("%v", val)
					break
				}
			}
		}
	}
	
	// If we still don't have a valid key, return an error
	if key == "" {
		return fmt.Errorf("could not extract a valid key from record data: %s", recordData)
	}

	// Initialize the schema's record map if it doesn't exist
	if _, exists := s.records[schemaName]; !exists {
		s.records[schemaName] = make(map[string]interface{})
	}

	// Store the record
	s.records[schemaName][key] = recordData

	// Update partial key index
	s.updatePartialKeyIndex(schemaName, key, true)

	return s.saveToPersistent()
}

// validateRecordAgainstSchema validates a record against the schema definition
func (s *Storage) validateRecordAgainstSchema(schemaName string, recordData string) error {
	schemaDef, exists := s.schemas[schemaName]
	if !exists {
		return fmt.Errorf("schema '%s' does not exist", schemaName)
	}

	// Parse the record data to validate
	var record map[string]interface{}
	if err := json.Unmarshal([]byte(recordData), &record); err != nil {
		return fmt.Errorf("invalid JSON format: %v", err)
	}

	// Parse the schema definition to check fields
	fields := parseSchemaFields(schemaDef)
	
	// Validate that all required fields in schema are present in record
	for field, fieldType := range fields {
		_, exists := record[field]
		if !exists {
			// For MVP, we'll allow optional fields but validate types for present fields
			continue
		}
		
		// Validate data type if field exists
		err := validateFieldType(record[field], fieldType)
		if err != nil {
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

// validateFieldType validates the value against the expected type
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