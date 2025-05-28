# Conduit SQL Processor

A powerful SQL processor for Conduit that enables SQL-based transformations, filtering, and enrichment of records flowing through data pipelines.

## Features

- **Multiple Processing Modes**: Transform, filter, or enrich records
- **Database Support**: PostgreSQL, MySQL, SQL Server, SQLite
- **In-Memory Operations**: Simple SQL-like operations without external database
- **Field Extraction**: Use record fields as query parameters
- **Flexible Configuration**: Extensive configuration options for various use cases

## Installation

### Building from Source

```bash
# Clone the repository
git clone https://github.com/your-org/conduit-processor-sql
cd conduit-processor-sql

# Build the WASM binary
make build

# This creates conduit-processor-sql.wasm
```

### Project Structure

```
conduit-processor-sql/
├── main.go                    # Entry point for WASM binary
├── sql/
│   ├── processor.go          # Main SQL processor implementation
│   ├── processor_test.go     # Comprehensive test suite
│   └── paramgen_proc.go      # Generated parameter definitions
├── go.mod
├── Makefile
└── README.md
```

### Using in Conduit

Copy the `conduit-processor-sql.wasm` file to your Conduit `processors` directory and reference it by its specification name in your pipeline configuration:

```yaml
processors:
  - id: transform-names
    plugin: sql.transform  # Uses the Name from Specification
    settings:
      sql.query: "SELECT name, UPPER(name) AS upper_name"
      sql.mode: "transform"
```

## Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `sql.query` | string | *required* | SQL query to execute |
| `sql.driver` | string | `postgres` | Database driver (postgres, mysql, sqlserver, sqlite3) |
| `sql.connectionString` | string | `""` | Database connection string |
| `sql.mode` | string | `transform` | Processing mode (transform, filter, enrich) |
| `sql.fields` | string | `""` | Comma-separated list of record fields to use as parameters |
| `sql.outputField` | string | `sql_result` | Field name for enrichment results |

## Usage Examples

### 1. Transform Mode - Field Transformation

Transform records by selecting and modifying fields:

```yaml
processors:
  - id: transform-names
    plugin: sql.transform
    settings:
      sql.query: "SELECT name, UPPER(name) AS upper_name, age + 1 AS next_age"
      sql.mode: "transform"
```

**Input Record:**
```json
{
  "name": "john doe",
  "age": 30,
  "city": "New York"
}
```

**Output Record:**
```json
{
  "name": "john doe",
  "upper_name": "JOHN DOE",
  "next_age": 31
}
```

### 2. Filter Mode - Conditional Record Filtering

Filter records based on SQL conditions:

```yaml
processors:
  - id: filter-adults
    plugin: sql.transform
    settings:
      sql.query: "SELECT age >= 18 AS is_adult WHERE age >= 18"
      sql.mode: "filter"
```

Only records where `age >= 18` will pass through the pipeline.

### 3. Enrich Mode - Database Lookup

Enrich records with data from external database:

```yaml
processors:
  - id: enrich-user-data
    plugin: sql.transform
    settings:
      sql.query: "SELECT email, phone, department FROM users WHERE user_id = $1"
      sql.mode: "enrich"
      sql.driver: "postgres"
      sql.connectionString: "postgres://user:pass@localhost/db"
      sql.fields: "user_id"
      sql.outputField: "user_details"
```

**Input Record:**
```json
{
  "user_id": 123,
  "action": "login"
}
```

**Output Record:**
```json
{
  "user_id": 123,
  "action": "login",
  "user_details": {
    "email": "user@example.com",
    "phone": "+1234567890",
    "department": "Engineering"
  }
}
```

### 4. Advanced Transform - String Operations

```yaml
processors:
  - id: transform-names
    plugin: sql.transform
    settings:
      sql.query: "SELECT first_name || ' ' || last_name AS full_name, email"
      sql.mode: "transform"
```

### 5. Database-based Filtering

Filter records using database lookups:

```yaml
processors:
  - id: filter-valid-users
    plugin: sql.transform
    settings:
      sql.query: "SELECT EXISTS(SELECT 1 FROM active_users WHERE id = $1)"
      sql.mode: "filter"
      sql.driver: "postgres"
      sql.connectionString: "postgres://user:pass@localhost/db"
      sql.fields: "user_id"
```

### 6. Complex Enrichment with Multiple Fields

```yaml
processors:
  - id: enrich-order-details
    plugin: sql.transform
    settings:
      sql.query: |
        SELECT 
          p.name as product_name,
          p.price,
          c.name as customer_name,
          c.email as customer_email
        FROM products p 
        JOIN customers c ON c.id = $2
        WHERE p.id = $1
      sql.mode: "enrich"
      sql.driver: "mysql"
      sql.connectionString: "user:pass@tcp(localhost:3306)/ecommerce"
      sql.fields: "product_id,customer_id"
      sql.outputField: "order_details"
```

## Database Connection Strings

### PostgreSQL
```
postgres://username:password@localhost:5432/database_name?sslmode=disable
```

### MySQL
```
username:password@tcp(localhost:3306)/database_name
```

### SQL Server
```
sqlserver://username:password@localhost:1433?database=database_name
```

### SQLite
```
file:path/to/database.db?cache=shared&mode=rwc
```

## In-Memory SQL Operations

When no `sql.connectionString` is provided, the processor supports basic SQL-like operations on record data:

### Supported Operations

- **SELECT**: Field selection and aliasing
- **UPDATE**: Field value updates
- **WHERE**: Simple filtering conditions
- **Expressions**: Basic arithmetic and string concatenation

### Examples

```sql
-- Select specific fields
SELECT name, age

-- Select with aliases
SELECT name AS full_name, age AS years_old

-- String concatenation
SELECT first_name || ' ' || last_name AS full_name

-- Arithmetic operations
SELECT age + 1 AS next_age

-- Update field values
UPDATE SET status = 'processed', updated_at = '2023-12-01'

-- Filter with conditions
SELECT * WHERE age >= 18
SELECT * WHERE status = 'active'
```

## Error Handling

The processor handles various error scenarios:

- **Invalid SQL**: Records with SQL parsing errors are marked as error records
- **Database Connection Issues**: Connection failures result in error records
- **Missing Fields**: Referenced fields that don't exist are treated as NULL
- **Type Mismatches**: Automatic type conversion where possible

## Performance Considerations

1. **Database Connections**: Connections are pooled and reused across records
2. **In-Memory Operations**: Faster for simple transformations without database lookups
3. **Batch Processing**: Records are processed individually but connections are shared
4. **Field Extraction**: Only extract fields that are actually used in queries

## Development

### Running Tests

```bash
make test
```

### Running with Coverage

```bash
make test-coverage
```

### Linting

```bash
make lint
```

### Building for Development

```bash
make build-dev
```

## Pipeline Configuration Examples

### Complete Pipeline with SQL Transformations

```yaml
version: 2.2
pipelines:
  - id: user-data-pipeline
    connectors:
      - id: source-postgres
        type: source
        plugin: postgres
        settings:
          connection: "postgres://user:pass@localhost/source_db"
          table: "user_events"
        processors:
          - id: transform-user-data
            plugin: sql.transform
            settings:
              sql.query: "SELECT user_id, event_type, UPPER(event_type) AS event_category"
              sql.mode: "transform"
          
      - id: destination-kafka
        type: destination
        plugin: kafka
        settings:
          brokers: "localhost:9092"
          topic: "processed-events"
        processors:
          - id: enrich-with-user-info
            plugin: sql.transform
            settings:
              sql.query: "SELECT name, email, tier FROM users WHERE id = $1"
              sql.mode: "enrich"
              sql.driver: "postgres"
              sql.connectionString: "postgres://user:pass@localhost/user_db"
              sql.fields: "user_id"
              sql.outputField: "user_info"
```

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Verify connection string format
   - Check database credentials and permissions
   - Ensure database is accessible from Conduit

2. **Query Execution Errors**
   - Validate SQL syntax
   - Ensure referenced fields exist in records
   - Check parameter count matches field list

3. **Performance Issues**
   - Use indexes on database columns used in WHERE clauses
   - Consider using in-memory operations for simple transformations
   - Monitor database connection pool usage

### Debugging

Enable debug logging in Conduit to see detailed processor execution:

```bash
CONDUIT_LOG_LEVEL=debug conduit run
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

This project is licensed under the Apache License 2.0 - see the LICENSE file for details.