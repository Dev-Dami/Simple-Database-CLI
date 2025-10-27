package config

import (
	"os"
	"path/filepath"
)

// Config holds the application configuration
type Config struct {
	StoragePath string // Path to the BSON storage file
	MaxKeys     int    // Maximum number of keys to store
}

// LoadConfig creates a default configuration
func LoadConfig() *Config {
	// Get current working directory
	wd, err := os.Getwd()
	if err != nil {
		// Default to current directory if error
		wd = "."
	}

	storagePath := filepath.Join(wd, "storage", "store.bson")
	
	return &Config{
		StoragePath: storagePath,
		MaxKeys:     10000, // Default maximum of 10,000 keys
	}
}