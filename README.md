# ClickHouse Import-Export App

This application facilitates the export and import of ClickHouse database schema and data. 
It consists of two main components: one for exporting data and another for importing data.

## Prerequisites

- Go version 1.22.2+
- [ClickHouse installation](https://clickhouse.com/docs/en/install)

## Installation

1. **Clone the repository**:
    ```bash
    git clone https://github.com/kankou-aliaksei/clickhouse-import-export.git
    cd clickhouse-import-export
    ```

2. **Ensure Go is installed**:
    Follow the instructions [here](https://golang.org/doc/install) to install Go if it is not already installed.

3. **Ensure ClickHouse is installed**:
    Follow the instructions [here](https://clickhouse.com/docs/en/install) to install ClickHouse if it is not already installed.

## Usage

### Export Data

To export data from a ClickHouse database, use the `export_data.go` script.

1. **Build and run the export script**:
    ```bash
    go run export_data.go \
        -host=mydb1 \
        -port=9000 \
        -user=admin \
        -password=your_password \
        -dbname=my_db \
        -chunkSize=1000000 \
        -clickhouseClientPath=../clickhouse_bin/clickhouse
    ```

### Import Data

To import data into a ClickHouse database, use the `import_data.go` script.

1. **Build and run the import script**:
    ```bash
    go run import_data.go \
        -host=mydb2 \
        -port=9000 \
        -user=admin \
        -password=your_password \
        -dbname=my_db \
        -clickhouseClientPath=../clickhouse_bin/clickhouse
    ```

## Configuration

Configuration for both scripts is done through command-line flags:

- `-host`: ClickHouse host
- `-port`: ClickHouse port
- `-user`: ClickHouse user
- `-password`: ClickHouse password
- `-dbname`: ClickHouse database name
- `-readTimeout`: Read timeout in seconds (default: 30)
- `-writeTimeout`: Write timeout in seconds (default: 30)
- `-chunkSize`: Number of rows to fetch per batch (only for export, default: 10000)
- `-clickhouseClientPath`: Path to the ClickHouse client executable (default: "clickhouse")

## Code Explanation

### `export_data.go`

This script exports the schema and data from a ClickHouse database.

1. **Load configuration from command-line flags**.
2. **Create and test the database connection**.
3. **Prepare directories for schema and data dumps**.
4. **Fetch all tables and process each one**:
    - Dump the schema of each table.
    - Dump the data of each table in batches using `clickhouse client`.

### `import_data.go`

This script imports the schema and data into a ClickHouse database.

1. **Load configuration from command-line flags**.
2. **Create and test the initial database connection**.
3. **Ensure the database exists**.
4. **Reconnect to the database with the specified database name**.
5. **Import schema and data**:
    - Import schema and views from the specified directory.
    - Import data for tables from the specified directory using `clickhouse client`.

## Example

### Export Data

```bash
go run export_data.go \
    -host=mydb1 \
    -port=9000 \
    -user=admin \
    -password=your_password \
    -dbname=my_db \
    -chunkSize=1000000 \
    -clickhouseClientPath=../clickhouse_bin/clickhouse
```

### Import Data

```bash
go run import_data.go \
    -host=mydb2 \
    -port=9000 \
    -user=admin \
    -password=your_password \
    -dbname=my_db \
    -clickhouseClientPath=../clickhouse_bin/clickhouse
```

## Notes

- Ensure the ClickHouse client executable path is correctly specified.
- For large datasets, adjust the `-chunkSize` flag to optimize performance.
