package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go"
)

// Config structure to hold the database configuration
type Config struct {
	Host                 string
	Port                 string
	User                 string
	Password             string
	DBName               string
	ReadTimeout          int
	WriteTimeout         int
	ClickHouseClientPath string
}

func main() {
	config := loadConfigFromFlags()
	log.Println(config)

	// Create and test the initial database connection
	db, err := createDBConnection(config, "")
	if err != nil {
		log.Fatalf("Initial database connection failed: %v", err)
	}
	defer db.Close()

	// Ensure the database exists
	if err := createDatabaseIfNotExists(db, config.DBName); err != nil {
		log.Fatalf("Failed to create database: %v", err)
	}

	// Reconnect to the database with the specified database name
	db, err = createDBConnection(config, config.DBName)
	if err != nil {
		log.Fatalf("Database connection to %s failed: %v", config.DBName, err)
	}
	defer db.Close()

	// Import schema and data
	if err := importData(db, "./schema", "./data", config); err != nil {
		log.Fatalf("Failed to import data: %v", err)
	}
}

// loadConfigFromFlags loads the configuration from command-line flags
func loadConfigFromFlags() Config {
	host := flag.String("host", "", "ClickHouse host")
	port := flag.String("port", "", "ClickHouse port")
	user := flag.String("user", "", "ClickHouse user")
	password := flag.String("password", "", "ClickHouse password")
	dbName := flag.String("dbname", "", "ClickHouse database name")
	readTimeout := flag.Int("readTimeout", 30, "Read timeout in seconds")
	writeTimeout := flag.Int("writeTimeout", 30, "Write timeout in seconds")
	clickHouseClientPath := flag.String("clickhouseClientPath", "clickhouse", "Path to ClickHouse client")
	flag.Parse()

	return Config{
		Host:                 *host,
		Port:                 *port,
		User:                 *user,
		Password:             *password,
		DBName:               *dbName,
		ReadTimeout:          *readTimeout,
		WriteTimeout:         *writeTimeout,
		ClickHouseClientPath: *clickHouseClientPath,
	}
}

// createDBConnection creates and tests a database connection
func createDBConnection(config Config, dbName string) (*sql.DB, error) {
	dsn := fmt.Sprintf("tcp://%s:%s?username=%s&password=%s&database=%s&read_timeout=%d&write_timeout=%d",
		config.Host, config.Port, config.User, config.Password, dbName, config.ReadTimeout, config.WriteTimeout)

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection to ClickHouse: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	log.Printf("Connection to ClickHouse %s successful.", dbName)
	return db, nil
}

// createDatabaseIfNotExists checks if the database exists and creates it if it does not
func createDatabaseIfNotExists(db *sql.DB, dbName string) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)
	if _, err := db.Exec(query); err != nil {
		return fmt.Errorf("failed to create database %s: %w", dbName, err)
	}
	return nil
}

// importData imports the schema and data from the specified directories
func importData(db *sql.DB, schemaDir, dataDir string, config Config) error {
	// Import schema and views
	if err := importSchema(db, schemaDir); err != nil {
		return err
	}

	// Import data for tables
	return importTableDataFromDir(db, dataDir, config)
}

// importSchema imports the schema from the specified directory
func importSchema(db *sql.DB, schemaDir string) error {
	schemaFiles, err := ioutil.ReadDir(schemaDir)
	if err != nil {
		return fmt.Errorf("failed to read schema directory: %w", err)
	}

	for _, file := range schemaFiles {
		if filepath.Ext(file.Name()) == ".sql" {
			schemaFilePath := filepath.Join(schemaDir, file.Name())
			schemaContent, err := ioutil.ReadFile(schemaFilePath)
			if err != nil {
				return fmt.Errorf("failed to read schema file %s: %w", schemaFilePath, err)
			}
			if _, err := db.Exec(string(schemaContent)); err != nil {
				return fmt.Errorf("failed to execute schema file %s: %w", schemaFilePath, err)
			}
			log.Printf("Schema imported for table/view %s", file.Name())
		}
	}
	return nil
}

// importTableDataFromDir imports data for tables from the specified directory
func importTableDataFromDir(db *sql.DB, dataDir string, config Config) error {
	dataFiles, err := ioutil.ReadDir(dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	for _, file := range dataFiles {
		if filepath.Ext(file.Name()) == ".tsv" {
			table := strings.TrimSuffix(file.Name(), filepath.Ext(file.Name()))
			dataFilePath := filepath.Join(dataDir, file.Name())
			if err := importTableData(config, table, dataFilePath, db); err != nil {
				log.Printf("Failed to import data for table %s: %v", table, err)
				continue // Skip this table and continue with the next one
			}
			log.Printf("Data imported for table %s", table)
		}
	}
	return nil
}

// importTableData imports data into the specified table using clickhouse-client
func importTableData(config Config, table, dataFilePath string, db *sql.DB) error {
	log.Printf("Importing data for table %s from file %s", table, dataFilePath)

	// Check if the table is a view
	isView, err := checkIfView(db, table, config.DBName)
	if err != nil {
		return fmt.Errorf("failed to check if table %s is a view: %w", table, err)
	}
	if isView {
		log.Printf("Skipping data import for view %s", table)
		return nil
	}

	// Check if the data file exists and is not empty
	fileInfo, err := os.Stat(dataFilePath)
	if os.IsNotExist(err) {
		log.Printf("Data file does not exist: %s", dataFilePath)
		return fmt.Errorf("data file does not exist: %s", dataFilePath)
	}
	if fileInfo.Size() == 0 {
		log.Printf("Data file is empty: %s", dataFilePath)
		return nil // Skip importing for empty data files
	}

	log.Printf("Data file %s exists and is not empty. Size: %d bytes", dataFilePath, fileInfo.Size())

	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		return fmt.Errorf("failed to open data file %s: %w", dataFilePath, err)
	}
	defer dataFile.Close()

	cmd := exec.Command(config.ClickHouseClientPath,
		"client",
		"--host", config.Host,
		"--port", config.Port,
		"--user", config.User,
		"--password", config.Password,
		"--query", fmt.Sprintf("INSERT INTO %s.%s FORMAT TSV", config.DBName, table),
	)
	cmd.Stdin = dataFile
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		// Log the problematic rows for debugging
		log.Printf("Error executing clickhouse-client: %v", err)
		return fmt.Errorf("failed to execute clickhouse-client: %w", err)
	}

	log.Printf("Data import for table %s completed successfully", table)
	return nil
}

// checkIfView checks if the specified table is a view
func checkIfView(db *sql.DB, table, dbName string) (bool, error) {
	query := fmt.Sprintf("SELECT engine FROM system.tables WHERE database = '%s' AND name = '%s'", dbName, table)
	var engine string
	if err := db.QueryRow(query).Scan(&engine); err != nil {
		return false, err
	}
	return engine == "View", nil
}
