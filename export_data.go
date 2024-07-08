package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"

	_ "github.com/ClickHouse/clickhouse-go"
)

type Config struct {
	Host                 string
	Port                 string
	User                 string
	Password             string
	DBName               string
	ReadTimeout          int
	WriteTimeout         int
	ChunkSize            int
	ClickHouseClientPath string
}

func main() {
	config := loadConfigFromFlags()
	log.Println(config)

	// Create and test the database connection
	db, err := createAndTestDBConnection(config)
	if err != nil {
		log.Fatalf("Database connection failed: %v", err)
	}
	defer db.Close()

	// Prepare directories for schema and data dumps
	schemaDir, dataDir := "./schema", "./data"
	createDirectories(schemaDir, dataDir)

	// Fetch all tables and process each one
	if err := processTables(db, config, schemaDir, dataDir); err != nil {
		log.Fatalf("Error processing tables: %v", err)
	}
}

// createAndTestDBConnection creates a DSN string, opens a database connection, and tests it
func createAndTestDBConnection(config Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("tcp://%s:%s?username=%s&password=%s&database=%s&read_timeout=%d&write_timeout=%d",
		config.Host, config.Port, config.User, config.Password, config.DBName, config.ReadTimeout, config.WriteTimeout)

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection to ClickHouse: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	log.Println("Connection to ClickHouse successful.")
	return db, nil
}

// createDirectories ensures the schema and data directories exist
func createDirectories(schemaDir, dataDir string) {
	if err := os.MkdirAll(schemaDir, 0755); err != nil {
		log.Fatalf("Failed to create schema directory: %v", err)
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}
}

// processTables fetches all tables and dumps their schema and data
func processTables(db *sql.DB, config Config, schemaDir, dataDir string) error {
	tables, err := getTables(db, config.DBName)
	if err != nil {
		return fmt.Errorf("failed to fetch tables: %w", err)
	}

	for _, table := range tables {
		if err := dumpTableSchema(db, config.DBName, table, schemaDir); err != nil {
			log.Printf("Error dumping schema for table %s: %v", table, err)
			continue
		}
		if err := dumpTableData(config, table, dataDir, db); err != nil {
			log.Printf("Error dumping data for table %s: %v", table, err)
			continue
		}
	}
	return nil
}

// loadConfigFromFlags loads the configuration for the ClickHouse client from command-line flags
func loadConfigFromFlags() Config {
	host := flag.String("host", "", "ClickHouse host")
	port := flag.String("port", "", "ClickHouse port")
	user := flag.String("user", "", "ClickHouse user")
	password := flag.String("password", "", "ClickHouse password")
	dbName := flag.String("dbname", "", "ClickHouse database name")
	readTimeout := flag.Int("readTimeout", 30, "Read timeout in seconds")
	writeTimeout := flag.Int("writeTimeout", 30, "Write timeout in seconds")
	chunkSize := flag.Int("chunkSize", 10000, "Number of rows to fetch per batch")
	clickHouseClientPath := flag.String("clickhouseClientPath", "clickhouse", "Path to the ClickHouse client executable")
	flag.Parse()

	return Config{
		Host:                 *host,
		Port:                 *port,
		User:                 *user,
		Password:             *password,
		DBName:               *dbName,
		ReadTimeout:          *readTimeout,
		WriteTimeout:         *writeTimeout,
		ChunkSize:            *chunkSize,
		ClickHouseClientPath: *clickHouseClientPath,
	}
}

// getTables fetches the list of tables in the specified database
func getTables(db *sql.DB, dbName string) ([]string, error) {
	query := fmt.Sprintf("SHOW TABLES FROM %s", dbName)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

// dumpTableSchema dumps the schema of the specified table
func dumpTableSchema(db *sql.DB, dbName, table, schemaDir string) error {
	query := fmt.Sprintf("SHOW CREATE TABLE %s.%s", dbName, table)
	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var createStmt string
	for rows.Next() {
		if err := rows.Scan(&createStmt); err != nil {
			return err
		}
	}

	schemaFile := fmt.Sprintf("%s/%s.sql", schemaDir, table)
	return os.WriteFile(schemaFile, []byte(createStmt), 0644)
}

// dumpTableData dumps the data of the specified table using clickhouse-client in batches and logs the progress
func dumpTableData(config Config, table, dataDir string, db *sql.DB) error {
	totalRows, err := getTotalRows(config.DBName, table, db)
	if err != nil {
		return err
	}

	dataFile, err := createDataFile(dataDir, table)
	if err != nil {
		return err
	}
	defer dataFile.Close()

	return exportTableData(config, table, dataFile, totalRows)
}

// getTotalRows returns the total number of rows in the specified table
func getTotalRows(dbName, table string, db *sql.DB) (int, error) {
	var totalRows int
	countQuery := fmt.Sprintf("SELECT count() FROM %s.%s", dbName, table)
	if err := db.QueryRow(countQuery).Scan(&totalRows); err != nil {
		return 0, err
	}
	return totalRows, nil
}

// createDataFile creates the data file for dumping the table data
func createDataFile(dataDir, table string) (*os.File, error) {
	dataFile := fmt.Sprintf("%s/%s.tsv", dataDir, table)
	return os.Create(dataFile)
}

// exportTableData exports the table data in batches and logs the progress
func exportTableData(config Config, table string, outputFile *os.File, totalRows int) error {
	offset := 0

	for offset < totalRows {
		if err := dumpBatch(config, table, outputFile, offset); err != nil {
			return err
		}

		offset += config.ChunkSize
		logProgress(table, offset, totalRows)
	}

	return nil
}

// dumpBatch executes the query to fetch a batch of data and writes it to the output file
func dumpBatch(config Config, table string, outputFile *os.File, offset int) error {
	query := fmt.Sprintf("SELECT * FROM %s.%s LIMIT %d OFFSET %d", config.DBName, table, config.ChunkSize, offset)
	cmd := exec.Command(config.ClickHouseClientPath,
		"client",
		"--host", config.Host,
		"--port", config.Port,
		"--user", config.User,
		"--password", config.Password,
		"--query", query,
		"--format", "TSV",
	)

	cmdOutput, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to execute clickhouse-client: %w", err)
	}

	if _, err := outputFile.Write(cmdOutput); err != nil {
		return fmt.Errorf("failed to write to output file: %w", err)
	}

	return nil
}

// logProgress logs the progress of the data export
func logProgress(table string, offset, totalRows int) {
	percentageExported := (float64(offset) / float64(totalRows)) * 100
	if percentageExported > 100 {
		percentageExported = 100
	}
	log.Printf("Export progress for table %s: %.2f%%", table, percentageExported)
}
