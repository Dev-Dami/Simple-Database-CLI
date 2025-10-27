package main

import (
	"fmt"
	"os"
	"strings"

	"simplebson/config"
	"simplebson/memory"
	"simplebson/preprocessing"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := strings.ToLower(os.Args[1])

	config := config.LoadConfig()

	storage := memory.NewStorage(config)

	args := os.Args[2:]
	parsedArgs, err := preprocessing.ParseCommand(command, args)
	if err != nil {
		fmt.Printf("Error parsing command: %v\n", err)
		os.Exit(1)
	}
	switch command {
	case "add":
		if len(parsedArgs) < 2 {
			fmt.Println("Usage: simplebson add <schema> <record_data>")
			os.Exit(1)
		}
		schema := parsedArgs[0]
		recordData := parsedArgs[1]
		err := storage.AddRecord(schema, recordData)
		if err != nil {
			fmt.Printf("Error adding record: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Record added successfully")

	case "get", "view":
		if len(parsedArgs) < 2 {
			fmt.Println("Usage: simplebson get <schema> <key>")
			os.Exit(1)
		}
		schema := parsedArgs[0]
		key := parsedArgs[1]
		record, err := storage.GetRecord(schema, key)
		if err != nil {
			fmt.Printf("Error retrieving record: %v\n", err)
			os.Exit(1)
		}
		fmt.Println(record)

	case "delete":
		if len(parsedArgs) < 2 {
			fmt.Println("Usage: simplebson delete <schema> <key>")
			os.Exit(1)
		}
		schema := parsedArgs[0]
		key := parsedArgs[1]
		err := storage.DeleteRecord(schema, key)
		if err != nil {
			fmt.Printf("Error deleting record: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Record deleted successfully")

	case "list":
		if len(parsedArgs) < 1 {
			fmt.Println("Usage: simplebson list <schema>")
			os.Exit(1)
		}
		schema := parsedArgs[0]
		records, err := storage.ListRecords(schema)
		if err != nil {
			fmt.Printf("Error listing records: %v\n", err)
			os.Exit(1)
		}
		for _, record := range records {
			fmt.Println(record)
		}

	case "schema":
		if len(parsedArgs) < 1 {
			schemas := storage.ListSchemas()
			if len(schemas) == 0 {
				fmt.Println("No schemas defined")
			} else {
				fmt.Println("Defined schemas:")
				for _, schema := range schemas {
					fmt.Printf("  %s\n", schema)
				}
			}
		} else if len(parsedArgs) == 1 {
			schema := parsedArgs[0]
			schemaDef, err := storage.GetSchema(schema)
			if err != nil {
				fmt.Printf("Error getting schema: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Schema '%s': %s\n", schema, schemaDef)
		} else {
			schema := parsedArgs[0]
			fieldsStr := strings.Join(parsedArgs[1:], " ")
			err := storage.CreateSchema(schema, fieldsStr)
			if err != nil {
				fmt.Printf("Error creating schema: %v\n", err)
				os.Exit(1)
			}
			fmt.Printf("Schema '%s' created successfully\n", schema)
		}

	default:
		fmt.Printf("Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  simplebson schema <schema_name> <field_definitions>  - Create or view schema")
	fmt.Println("  simplebson add <schema> <record_data>               - Add a record")
	fmt.Println("  simplebson get <schema> <key>                      - Get a record")
	fmt.Println("  simplebson view <schema> <key>                     - View a record")
	fmt.Println("  simplebson delete <schema> <key>                   - Delete a record")
	fmt.Println("  simplebson list <schema>                           - List all records of a schema")
	fmt.Println("")
	fmt.Println("Examples:")
	fmt.Println("  simplebson schema User name:string age:int email:string")
	fmt.Println("  simplebson add User '{\"name\":\"Alice\", \"age\":30, \"email\":\"alice@example.com\"}'")
	fmt.Println("  simplebson get User Alice")
	fmt.Println("  simplebson list User")
	fmt.Println("  simplebson delete User Alice")
}
