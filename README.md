# Conduit SQL Processor (WASM Compatible)

A high-performance, WASM-compatible SQL processor for [Conduit](https://conduit.io) that enables SQL-based transformations and filtering of streaming data records using in-memory operations.

## Quick Start

```bash
# 1. Clone and build
git clone https://github.com/devarispbrown/conduit-processor-sql
cd conduit-processor-sql
make build

# 2. Copy to Conduit
cp conduit-processor-sql.wasm /path/to/conduit/processors/

# 3. Use in pipeline
```

```yaml
processors:
  - id: transform-users
    plugin: sql.transform
    settings:
      query: "SELECT name, UPPER(email) AS email_upper, age + 1 AS next_age"
      mode: "transform"
```

## Features

### ✅ **WASM Compatible**
- Runs in WebAssembly environments without network dependencies
- High-performance in-memory operations
- No external database connections required

### ✅ **Comprehensive SQL Support**
- **SELECT**: Field selection, aliases, expressions, WHERE clauses
- **UPDATE**: Field updates with conditional logic
- **Functions**: UPPER(), LOWER(), LENGTH() 
- **Operators**: Arithmetic (+,-,*,/), concatenation (||), comparisons (=,<,>,etc), logical (AND,OR)
- **Data Types**: Strings, numbers, booleans, NULL

### ✅ **Production Ready**
- Batch processing for high throughput
- Memory pooling for efficiency  
- Input validation and security features
- Comprehensive error handling
- Performance monitoring and profiling

## Installation

### Prerequisites

- **Go**: 1.24.2 or later
- **Make**: For build automation
- **Git**: For version control

### Quick Install

```bash
# Install tools and build
make install-tools
make build
```

### Manual Build

```bash
# Clone repository
git clone https://github.com/devarispbrown/conduit-processor-sql
cd conduit-processor-sql

# Build WASM binary for Conduit
GOOS=wasip1 GOARCH=wasm CGO_ENABLED=0 go build -tags wasm -o conduit-processor-sql.wasm ./cmd/processor/main.go
```

## Configuration

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | string | *required* | SQL query to execute (max 10,000 chars) |
| `mode` | string | `transform` | Processing mode: `transform` or `filter` |
| `fields` | string | `""` | Comma-separated field names for parameter extraction |
| `batchSize` | int | `100` | Records per batch (1-10000) |

### Validation Rules

- Query length: 1-10,000 characters
- Field count: Maximum 50 fields
- Field names: Alphanumeric, underscore, dot only
- Batch size: 1-10,000 records

## SQL Reference

### Supported Statements

#### SELECT - Transform Records
```sql
-- Basic selection
SELECT name, age, email

-- With aliases
SELECT name AS full_name, age AS years_old

-- All fields
SELECT *

-- With WHERE clause
SELECT name, age WHERE age >= 18

-- Complex expressions
SELECT first_name || ' ' || last_name AS full_name,
       UPPER(email) AS email_upper,
       salary * 1.1 AS new_salary
WHERE department = 'Engineering' AND status = 'active'
```

#### UPDATE - Modify Fields
```sql
-- Simple updates
UPDATE SET processed = true, updated_at = '2024-01-01'

-- Conditional updates
UPDATE SET bonus = salary * 0.1 WHERE performance_rating >= 4

-- Multiple field updates
UPDATE SET status = 'processed', 
           score = score + 10,
           last_modified = 'today'
WHERE category = 'urgent'
```

### Functions

| Function | Description | Example |
|----------|-------------|---------|
| `UPPER(text)` | Convert to uppercase | `UPPER(name)` → `"JOHN"` |
| `LOWER(text)` | Convert to lowercase | `LOWER(email)` → `"user@example.com"` |
| `LENGTH(text)` | Get string length | `LENGTH(name)` → `4` |

### Operators

#### Arithmetic
- `+` Addition: `age + 1`
- `-` Subtraction: `total - discount`
- `*` Multiplication: `price * quantity`
- `/` Division: `total / count`

#### String
- `||` Concatenation: `first_name || ' ' || last_name`

#### Comparison
- `=` Equal: `status = 'active'`
- `<>`, `!=` Not equal: `status <> 'inactive'`
- `<`, `<=`, `>`, `>=` Comparison: `age >= 18`

#### Logical
- `AND` Both conditions: `age >= 18 AND status = 'active'`
- `OR` Either condition: `role = 'admin' OR department = 'security'`

### Data Types

```sql
-- Strings (single or double quotes)
'Hello World'
"Another string"

-- Numbers (integer or decimal)
42
3.14159
-100

-- Booleans
true
false

-- NULL
NULL
```

## Usage Examples

### 1. User Data Transformation

**Transform user records with name formatting and email normalization:**

```yaml
processors:
  - id: transform-users
    plugin: sql.transform
    settings:
      query: |
        SELECT 
          first_name || ' ' || last_name AS full_name,
          LOWER(email) AS email,
          age,
          UPPER(status) AS status_code
        WHERE age >= 18
      mode: "transform"
```

**Input:**
```json
{
  "first_name": "John",
  "last_name": "Doe",
  "email": "JOHN.DOE@COMPANY.COM",
  "age": 30,
  "status": "active"
}
```

**Output:**
```json
{
  "full_name": "John Doe",
  "email": "john.doe@company.com", 
  "age": 30,
  "status_code": "ACTIVE"
}
```

### 2. E-commerce Order Processing

**Calculate order totals and apply business rules:**

```yaml
processors:
  - id: process-orders
    plugin: sql.transform
    settings:
      query: |
        UPDATE SET 
          total = price * quantity,
          tax = price * quantity * 0.08,
          final_total = (price * quantity) * 1.08,
          priority = 'high'
        WHERE category = 'electronics' AND price > 100
      mode: "transform"
```

### 3. Event Filtering

**Filter events based on complex business logic:**

```yaml
processors:
  - id: filter-important-events
    plugin: sql.transform
    settings:
      query: |
        SELECT * 
        WHERE (event_type = 'error' AND severity >= 3) 
           OR (event_type = 'warning' AND user_role = 'admin')
           OR event_type = 'security_alert'
      mode: "filter"
```

### 4. Data Enrichment

**Add computed fields and normalize data:**

```yaml
processors:
  - id: enrich-metrics
    plugin: sql.transform
    settings:
      query: |
        SELECT *,
          LENGTH(description) AS desc_length,
          timestamp || '_processed' AS processing_id,
          CASE 
            WHEN score >= 90 THEN 'excellent'
            WHEN score >= 70 THEN 'good' 
            ELSE 'needs_improvement'
          END AS rating
      mode: "transform"
```

### 5. High-Volume Batch Processing

**Process large datasets efficiently:**

```yaml
processors:
  - id: batch-processor
    plugin: sql.transform
    settings:
      query: "SELECT id, UPPER(name) AS name_normalized, processed = true"
      mode: "transform"
      batchSize: 1000  # Process 1000 records at once
```

## Development

### Quick Development Setup

```bash
# 1. Install dependencies
make install-tools

# 2. Run tests
make test

# 3. Development cycle
make dev          # fmt + test + build
```

### Code Structure

The processor follows Conduit SDK conventions:

```go
// Main processor struct
type Processor struct {
    sdk.UnimplementedProcessor
    config ProcessorConfig
    // ... other fields
}

// Required SDK methods
func (p *Processor) Specification() (sdk.Specification, error)
func (p *Processor) Configure(ctx context.Context, cfg config.Config) error  
func (p *Processor) Open(ctx context.Context) error
func (p *Processor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord
func (p *Processor) Teardown(ctx context.Context) error
```

### Available Commands

#### Essential Commands
```bash
make build        # Build WASM binary
make test         # Run all tests  
make check        # Run quality checks
make clean        # Clean artifacts
```

#### Development Commands
```bash
make dev          # Quick development cycle
make test-coverage # Test with coverage
make lint         # Run linter
make fmt          # Format code
```

#### Performance Commands
```bash
make bench        # Run benchmarks
make profile-mem  # Memory profiling
make profile-cpu  # CPU profiling
```

#### Testing Commands
```bash
make test                    # All tests
make test-sql               # SQL operation tests
make test GOTEST_FLAGS="-v" # Verbose testing
```

### Testing

#### Run Tests
```bash
# Basic testing
make test

# With coverage
make coverage

# Specific components  
make test-sql           # SQL operations
make test-expressions   # Expression evaluation
make test-transforms    # Record transformations
```

#### Test Structure
Tests follow Conduit SDK patterns and cover:

- **Configuration validation** - All parameter combinations
- **SQL operations** - SELECT, UPDATE, WHERE clause evaluation  
- **Expression evaluation** - Functions, operators, data types
- **Error handling** - Invalid queries, malformed data, security
- **Performance** - Benchmarks for critical code paths

```bash
# Run specific test categories
go test -run TestProcessor_Configure     # Configuration tests
go test -run TestProcessor_Transform     # Transform operation tests  
go test -run TestProcessor_Filter        # Filter operation tests
go test -run TestProcessor_Expression    # Expression evaluation tests
```

#### Benchmarking
```bash
# Performance benchmarks
make bench

# Specific benchmarks
go test -bench=BenchmarkSQLProcessor_Process -benchmem
go test -bench=BenchmarkSQLProcessor_EvaluateExpression -benchmem
```

## WASM Limitations

### ✅ Supported
- **Transform Mode**: Complete SQL SELECT/UPDATE operations
- **Filter Mode**: Complex WHERE clause evaluation
- **In-Memory Processing**: All operations use memory only
- **SQL Functions**: UPPER, LOWER, LENGTH
- **All Operators**: Arithmetic, string, comparison, logical
- **Performance Features**: Batch processing, memory pooling

### ❌ Not Supported
- **Enrich Mode**: No database connectivity
- **Network Operations**: No external API calls
- **File I/O**: No direct file system access
- **Advanced SQL**: JOINs, subqueries, window functions
- **Database Functions**: NOW(), RANDOM(), database-specific functions

## Troubleshooting

### Common Issues

#### 1. Query Syntax Errors
```
Error: failed to apply transformations: invalid condition
```
**Solutions:**
- Check SQL syntax carefully
- Ensure proper spacing around operators: `age >= 18` not `age>=18`
- Use single quotes for strings: `'value'` not `value`
- Validate parentheses matching in complex conditions

#### 2. Field Reference Errors
```
Error: failed to parse record data: field 'user_name' not found
```
**Solutions:**
- Verify field names match exactly (case-sensitive)
- Check input record structure
- Use debug logging: `CONDUIT_LOG_LEVEL=debug`

#### 3. Type Mismatch Errors
```
Error: cannot perform arithmetic on non-numeric values
```
**Solutions:**
- Ensure arithmetic operations use numeric fields
- Check data types in input records
- Use string concatenation (`||`) for text operations

#### 4. Performance Issues
```
Processing is slow with large batches
```
**Solutions:**
- Tune batch size: Start with 100, increase to 1000 for high throughput
- Simplify complex expressions
- Use memory profiling: `make profile-mem`

### Debug Commands

```bash
# Enable debug logging
CONDUIT_LOG_LEVEL=debug conduit run pipeline.yaml

# Profile memory usage
make profile-mem

# Profile CPU usage  
make profile-cpu

# Benchmark performance
make bench
```

### Performance Tuning

1. **Batch Size Optimization**
   ```yaml
   settings:
     batchSize: 1000  # Increase for high throughput
   ```

2. **Query Optimization**
   ```sql
   -- Good: Simple field selection
   SELECT name, age WHERE status = 'active'
   
   -- Avoid: Complex nested expressions
   SELECT UPPER(LOWER(UPPER(name))) AS name
   ```

3. **Memory Management**
   ```bash
   # Monitor memory usage
   make profile-mem
   
   # Check for memory leaks
   go test -memprofile=mem.prof -bench=.
   ```

## Pipeline Examples

### Simple Transformation Pipeline
```yaml
version: 2.2
pipelines:
  - id: user-processing
    connectors:
      - id: source
        type: source
        plugin: file
        settings:
          path: "./users.json"
        processors:
          - id: transform
            plugin: sql.transform
            settings:
              query: "SELECT name, UPPER(email) AS email_upper"
              mode: "transform"
      - id: dest
        type: destination
        plugin: file
        settings:
          path: "./processed.json"
```

### Multi-Stage Processing Pipeline
```yaml
version: 2.2
pipelines:
  - id: complex-processing
    connectors:
      - id: events-source
        type: source
        plugin: kafka
        settings:
          brokers: "localhost:9092"
          topic: "raw-events"
        processors:
          # Stage 1: Normalize
          - id: normalize
            plugin: sql.transform
            settings:
              query: "SELECT UPPER(user_id) AS user_id, LOWER(email) AS email, event_type"
              mode: "transform"
          
          # Stage 2: Filter
          - id: filter-valid
            plugin: sql.transform
            settings:
              query: "SELECT * WHERE LENGTH(user_id) > 0 AND email != NULL"
              mode: "filter"
          
          # Stage 3: Enrich
          - id: add-metadata
            plugin: sql.transform
            settings:
              query: "UPDATE SET processed = true, processing_time = '2024-01-01'"
              mode: "transform"
              batchSize: 500
      
      - id: processed-dest
        type: destination
        plugin: kafka
        settings:
          brokers: "localhost:9092"
          topic: "processed-events"
```

## Contributing

### Development Workflow

1. **Setup**
   ```bash
   git clone https://github.com/devarispbrown/conduit-processor-sql
   cd conduit-processor-sql
   make install-tools
   ```

2. **Development**
   ```bash
   # Make changes
   make dev          # Test and build
   make check        # Full quality check
   ```

3. **Submit**
   ```bash
   git add .
   git commit -m "feat: add new feature"
   git push origin feature-branch
   # Create pull request
   ```

### Guidelines

- **Tests**: Add comprehensive tests for new features
- **Documentation**: Update README for user-facing changes  
- **Code Quality**: Run `make check` before submitting
- **WASM Compatibility**: Ensure no network dependencies
- **Performance**: Add benchmarks for critical code paths

### Testing New Features

```bash
# Test your changes
make test-sql               # SQL operations
make test-expressions       # Expression evaluation
make bench                  # Performance impact
make test-coverage          # Coverage verification
```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE.md](LICENSE.md) file for details.

## Support

- **Issues**: [GitHub Issues](https://github.com/devarispbrown/conduit-processor-sql/issues)
- **Discussions**: [GitHub Discussions](https://github.com/devarispbrown/conduit-processor-sql/discussions)  
- **Documentation**: [Conduit Documentation](https://conduit.io/docs)
- **Community**: [Conduit Discord](https://discord.gg/conduit)