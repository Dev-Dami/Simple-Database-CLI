# SimpleBSONDB CLI

SimpleBSONDB is a lightweight Go-based CLI database for storing user or arbitrary records in a schema-driven format. It allows flexible schema creation, storage, and retrieval operations with a simple command syntax.

## Features

* Store structured records with flexible schemas
* Fast key-based search using partial key matching (first 5 characters)
* JSON record validation against schema definitions
* Persistent storage with automatic saving
* CLI commands for managing database records

## Installation

1. Make sure you have Go 1.25+ installed
2. Clone the repository
3. Run `go build` to build the executable

## Usage

```
# Define a schema
simplebson schema <schema_name> <field_definitions>

# Add a record
simplebson add <schema> <record_data>

# Retrieve a record by full or partial key
simplebson get <schema> <key>
simplebson view <schema> <key>  # alias for get

# Delete a record
simplebson delete <schema> <key>

# List all records of a schema
simplebson list <schema>

# View schema definition
simplebson schema <schema_name>

# List all schemas
simplebson schema
```

## Schema Definition

When defining a schema, specify field names and types in the format `fieldname:type`:
- `string` - text values
- `int` or `integer` - whole numbers
- `float` or `double` - decimal numbers  
- `bool` or `boolean` - true/false values
- `object` or `json` - nested objects (no validation)

Example: `simplebson schema User name:string age:int email:string`

## Examples

```bash
# Create a User schema
simplebson schema User name:string age:int email:string

# Add users
simplebson add User "{\"name\":\"Alice\", \"age\":30, \"email\":\"alice@example.com\"}"
simplebson add User "{\"name\":\"Bob\", \"age\":25, \"email\":\"bob@example.com\"}"
simplebson add User "{\"name\":\"Alicia\", \"age\":28, \"email\":\"alicia@example.com\"}"

# Retrieve users (using full key)
simplebson get User Alice

# Retrieve using partial key (first 5 characters or less)
simplebson get User Ali   # Will match Alice or Alicia (error if multiple matches)
simplebson get User Bo    # Will match Bob

# List all users
simplebson list User

# View schema
simplebson schema User

# List all schemas
simplebson schema

# Delete a user
simplebson delete User Alice

# Create another schema
simplebson schema Product id:string name:string price:float
simplebson add Product "{\"id\":\"P001\", \"name\":\"Laptop\", \"price\":999.99}"
```

## Partial Key Matching

SimpleBSONDB implements fast key-based search using partial key matching. When you search with a key:
- If the key is 5+ characters, it matches the first 5 characters
- If the key is fewer than 5 characters, it matches keys that start with that prefix
- If multiple records match, an error is returned asking for a more specific key

## Data Validation

When adding records, SimpleBSONDB validates:
- JSON format validity
- Field types according to the schema definition
- Required schema existence

## Storage

Data is automatically persisted to `storage/store.bson` in binary BSON format. The database consists of:
- Records stored by schema and key
- Schema definitions stored separately
- Automatic saving after each operation

## Future Enhancement: Multiple BSON Files

We plan to enhance SimpleBSONDB to allow users to create and manage their own `.bson` files, similar to how SQLite allows multiple database files. This will provide:
- Multiple database files for different purposes
- Better data organization and separation
- Portability of individual data sets
- The ability to work with multiple BSON database files simultaneously

Example future usage:
```bash
# Create a new database file
simplebson -db mydata.bson schema User name:string age:int

# Work with different database files
simplebson -db users.bson add User "{\"name\":\"Alice\", \"age\":30}"
simplebson -db products.bson add Product "{\"id\":\"P001\", \"name\":\"Laptop\"}"
```

This enhancement will make SimpleBSONDB as flexible as SQLite while maintaining its simplicity and BSON efficiency.
