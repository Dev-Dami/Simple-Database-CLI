package config

import (
	"os"
	"path/filepath"
)

// Config holds the application configuration
type Config struct {
	StoragePath string
	MaxKeys     int
}

// LoadConfig creates a default configuration
func LoadConfig() *Config {
	wd, err := os.Getwd()
	if err != nil {
		wd = "."
	}

	storagePath := filepath.Join(wd, "dbs", "default", "db.bson")

	return &Config{
		StoragePath: storagePath,
		MaxKeys:     10000,
	}
}
