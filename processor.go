//go:build wasm

package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"

	// SQL drivers - add as needed
	_ "github.com/go-sql-driver/mysql"  // MySQL
	_ "github.com/lib/pq"               // PostgreSQL
	_ "github.com/mattn/go-sqlite3"     // SQLite
	_ "github.com/microsoft/go-mssqldb" // SQL Server
)

//go:generate paramgen -output=paramgen_proc.go ProcessorConfig

const (
	// Performance constants
	defaultMaxOpenConns    = 10
	defaultMaxIdleConns    = 5
	defaultConnMaxLifetime = 30 * time.Minute
	defaultQueryTimeout    = 30 * time.Second
	defaultBatchSize       = 100

	// Security constants
	maxQueryLength     = 10000
	maxFieldCount      = 50
	maxConnectionRetry = 3
)

type SQLProcessor struct {
	sdk.UnimplementedProcessor

	config       ProcessorConfig
	db           *sql.DB
	fields       []string
	preparedStmt *sql.Stmt

	// Performance optimizations
	recordPool    sync.Pool
	stringBuilder strings.Builder

	// Security and monitoring
	queryTimeouts map[string]time.Duration
	mu            sync.RWMutex
	metrics       ProcessorMetrics
}

type ProcessorConfig struct {
	// SQL query to execute
	Query string `json:"query" validate:"required,max=10000"`
	// SQL driver to use
	Driver string `json:"driver" default:"postgres"`
	// Database connection string (will be redacted in logs)
	ConnectionString string `json:"connectionString" validate:"required_if=Mode enrich"`
	// Processing mode
	Mode string `json:"mode" default:"transform"`
	// Fields to extract from record
	Fields string `json:"fields" validate:"max=500"`
	// Output field for enrichment mode
	OutputField string `json:"outputField" default:"sql_result"`
	// Connection pool settings
	MaxOpenConns    int           `json:"maxOpenConns" default:"10"`
	MaxIdleConns    int           `json:"maxIdleConns" default:"5"`
	ConnMaxLifetime time.Duration `json:"connMaxLifetime" default:"30m"`
	QueryTimeout    time.Duration `json:"queryTimeout" default:"30s"`
	// Batch processing
	BatchSize int `json:"batchSize" default:"100"`
}

type ProcessorMetrics struct {
	recordsProcessed   int64
	queriesExecuted    int64
	queryErrors        int64
	totalQueryDuration time.Duration
	mu                 sync.RWMutex
}

func (m *ProcessorMetrics) recordQuery(duration time.Duration, success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.queriesExecuted++
	m.totalQueryDuration += duration
	if !success {
		m.queryErrors++
	}
}

func (m *ProcessorMetrics) recordProcessed(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recordsProcessed += int64(count)
}

func (m *ProcessorMetrics) getStats() (int64, int64, int64, time.Duration) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.recordsProcessed, m.queriesExecuted, m.queryErrors, m.totalQueryDuration
}

func (p *SQLProcessor) Specification() (sdk.Specification, error) {
	return sdk.Specification{
		Name:    "sql.transform",
		Summary: "Transform records using SQL queries with enterprise features",
		Description: `Production-ready SQL processor with connection pooling, security features, 
performance optimizations, and comprehensive monitoring. Supports multiple SQL databases 
and can execute both simple transformations and complex queries with joins to external data sources.`,
		Version:    "v2.0.0",
		Author:     "Conduit Community",
		Parameters: ProcessorConfig{}.Parameters(),
	}, nil
}

func (p *SQLProcessor) Configure(ctx context.Context, cfg config.Config) error {
	logger := sdk.Logger(ctx)

	err := cfg.DecodeInto(&p.config)
	if err != nil {
		logger.Error().Err(err).Msg("failed to parse configuration")
		return fmt.Errorf("failed to parse configuration: %w", err)
	}

	// Security: Validate configuration
	if err := p.validateConfig(); err != nil {
		logger.Error().Err(err).Msg("configuration validation failed")
		return fmt.Errorf("configuration validation failed: %w", err)
	}

	// Parse and validate fields list
	if p.config.Fields != "" {
		fields := strings.Split(strings.ReplaceAll(p.config.Fields, " ", ""), ",")
		if len(fields) > maxFieldCount {
			return fmt.Errorf("too many fields specified (max %d): %d", maxFieldCount, len(fields))
		}

		// Security: Validate field names
		for _, field := range fields {
			if !isValidFieldName(field) {
				return fmt.Errorf("invalid field name: %s", field)
			}
		}
		p.fields = fields
	}

	// Initialize performance optimizations
	p.recordPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]interface{})
		},
	}

	p.queryTimeouts = make(map[string]time.Duration)

	logger.Info().
		Str("mode", p.config.Mode).
		Str("driver", p.config.Driver).
		Int("fields_count", len(p.fields)).
		Msg("SQL processor configured successfully")

	return nil
}

func (p *SQLProcessor) Open(ctx context.Context) error {
	logger := sdk.Logger(ctx)

	// Only initialize database connection if connection string is provided
	if p.config.ConnectionString == "" {
		logger.Info().Msg("no database connection string provided, using in-memory processing only")
		return nil
	}

	// Security: Redact connection string in logs
	redactedConnStr := redactConnectionString(p.config.ConnectionString)
	logger.Info().
		Str("driver", p.config.Driver).
		Str("connection", redactedConnStr).
		Msg("establishing database connection")

	// Performance: Configure connection pool
	db, err := sql.Open(p.config.Driver, p.config.ConnectionString)
	if err != nil {
		logger.Error().Err(err).Msg("failed to open database connection")
		return fmt.Errorf("failed to open database connection: %w", err)
	}

	// Performance: Set connection pool parameters
	db.SetMaxOpenConns(p.config.MaxOpenConns)
	db.SetMaxIdleConns(p.config.MaxIdleConns)
	db.SetConnMaxLifetime(p.config.ConnMaxLifetime)

	// Test connection with timeout and retry logic
	for attempt := 1; attempt <= maxConnectionRetry; attempt++ {
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err = db.PingContext(pingCtx)
		cancel()

		if err == nil {
			break
		}

		logger.Warn().
			Err(err).
			Int("attempt", attempt).
			Int("max_attempts", maxConnectionRetry).
			Msg("database ping failed, retrying")

		if attempt == maxConnectionRetry {
			db.Close()
			return fmt.Errorf("failed to ping database after %d attempts: %w", maxConnectionRetry, err)
		}

		// Exponential backoff
		time.Sleep(time.Duration(attempt) * time.Second)
	}

	p.db = db

	// Performance: Prepare statement if possible
	if p.config.Mode != "transform" || strings.Contains(p.config.Query, "$") {
		stmt, err := p.db.PrepareContext(ctx, p.config.Query)
		if err != nil {
			logger.Warn().Err(err).Msg("failed to prepare statement, will use direct queries")
		} else {
			p.preparedStmt = stmt
			logger.Debug().Msg("prepared statement created successfully")
		}
	}

	logger.Info().Msg("database connection established successfully")
	return nil
}

func (p *SQLProcessor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	logger := sdk.Logger(ctx)

	start := time.Now()
	result := make([]sdk.ProcessedRecord, len(records))

	// Performance: Process in batches if configured
	if p.config.BatchSize > 1 && len(records) > p.config.BatchSize {
		return p.processBatched(ctx, records)
	}

	successCount := 0
	errorCount := 0

	for i, record := range records {
		recordStart := time.Now()

		processed, err := p.processRecord(ctx, record)
		if err != nil {
			logger.Debug().
				Err(err).
				Int("record_index", i).
				Str("record_id", string(record.Position)).
				Msg("failed to process record")

			result[i] = sdk.ErrorRecord{Error: err}
			errorCount++
			continue
		}

		result[i] = processed
		successCount++

		// Performance logging at trace level to avoid hot path impact
		logger.Trace().
			Int("record_index", i).
			Dur("duration", time.Since(recordStart)).
			Msg("record processed successfully")
	}

	processingDuration := time.Since(start)

	// Update metrics
	p.metrics.recordProcessed(len(records))

	// Log batch summary
	logger.Debug().
		Int("total_records", len(records)).
		Int("successful", successCount).
		Int("errors", errorCount).
		Dur("total_duration", processingDuration).
		Dur("avg_per_record", processingDuration/time.Duration(len(records))).
		Msg("batch processing completed")

	return result
}

func (p *SQLProcessor) processBatched(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	logger := sdk.Logger(ctx)

	result := make([]sdk.ProcessedRecord, len(records))
	batchSize := p.config.BatchSize

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}

		batch := records[i:end]
		batchResults := make([]sdk.ProcessedRecord, len(batch))

		// Process batch
		for j, record := range batch {
			processed, err := p.processRecord(ctx, record)
			if err != nil {
				batchResults[j] = sdk.ErrorRecord{Error: err}
				continue
			}
			batchResults[j] = processed
		}

		// Copy batch results to main result
		copy(result[i:end], batchResults)

		logger.Trace().
			Int("batch_start", i).
			Int("batch_size", len(batch)).
			Msg("batch processed")
	}

	return result
}

func (p *SQLProcessor) processRecord(ctx context.Context, record opencdc.Record) (sdk.ProcessedRecord, error) {
	// Security: Validate record size
	if len(record.Payload.After.Bytes()) > 1024*1024 { // 1MB limit
		return nil, fmt.Errorf("record payload too large: %d bytes", len(record.Payload.After.Bytes()))
	}

	switch p.config.Mode {
	case "transform":
		return p.transformRecord(ctx, record)
	case "filter":
		return p.filterRecord(ctx, record)
	case "enrich":
		return p.enrichRecord(ctx, record)
	default:
		return nil, fmt.Errorf("unknown processing mode: %s", p.config.Mode)
	}
}

func (p *SQLProcessor) transformRecord(ctx context.Context, record opencdc.Record) (sdk.ProcessedRecord, error) {
	// For transform mode, prefer in-memory processing for performance
	if p.db == nil {
		return p.transformInMemory(record)
	}

	return p.transformWithDatabase(ctx, record)
}

func (p *SQLProcessor) transformWithDatabase(ctx context.Context, record opencdc.Record) (sdk.ProcessedRecord, error) {
	logger := sdk.Logger(ctx)

	// Performance: Extract field values with pool
	args, err := p.extractFieldValuesOptimized(record)
	if err != nil {
		return nil, fmt.Errorf("failed to extract field values: %w", err)
	}

	// Security: Add query timeout
	queryCtx, cancel := context.WithTimeout(ctx, p.config.QueryTimeout)
	defer cancel()

	queryStart := time.Now()

	var rows *sql.Rows
	if p.preparedStmt != nil {
		rows, err = p.preparedStmt.QueryContext(queryCtx, args...)
	} else {
		rows, err = p.db.QueryContext(queryCtx, p.config.Query, args...)
	}

	queryDuration := time.Since(queryStart)
	p.metrics.recordQuery(queryDuration, err == nil)

	if err != nil {
		logger.Error().
			Err(err).
			Dur("query_duration", queryDuration).
			Msg("database query failed")
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Performance: Process results efficiently
	updatedRecord, err := p.applyQueryResultsOptimized(record, rows)
	if err != nil {
		return nil, fmt.Errorf("failed to apply query results: %w", err)
	}

	logger.Trace().
		Dur("query_duration", queryDuration).
		Msg("database transform completed")

	return sdk.SingleRecord(updatedRecord), nil
}

func (p *SQLProcessor) filterRecord(ctx context.Context, record opencdc.Record) (sdk.ProcessedRecord, error) {
	if p.db == nil {
		// In-memory filtering
		include, err := p.filterInMemoryOptimized(record)
		if err != nil {
			return nil, err
		}
		if include {
			return sdk.SingleRecord(record), nil
		}
		return sdk.FilterRecord{}, nil
	}

	// Database-based filtering with timeout
	queryCtx, cancel := context.WithTimeout(ctx, p.config.QueryTimeout)
	defer cancel()

	args, err := p.extractFieldValuesOptimized(record)
	if err != nil {
		return nil, fmt.Errorf("failed to extract field values: %w", err)
	}

	queryStart := time.Now()
	var result bool

	if p.preparedStmt != nil {
		err = p.preparedStmt.QueryRowContext(queryCtx, args...).Scan(&result)
	} else {
		err = p.db.QueryRowContext(queryCtx, p.config.Query, args...).Scan(&result)
	}

	queryDuration := time.Since(queryStart)
	p.metrics.recordQuery(queryDuration, err == nil)

	if err != nil {
		return nil, fmt.Errorf("failed to execute filter query: %w", err)
	}

	if result {
		return sdk.SingleRecord(record), nil
	}
	return sdk.FilterRecord{}, nil
}

func (p *SQLProcessor) enrichRecord(ctx context.Context, record opencdc.Record) (sdk.ProcessedRecord, error) {
	if p.db == nil {
		return nil, fmt.Errorf("enrich mode requires database connection")
	}

	logger := sdk.Logger(ctx)

	queryCtx, cancel := context.WithTimeout(ctx, p.config.QueryTimeout)
	defer cancel()

	args, err := p.extractFieldValuesOptimized(record)
	if err != nil {
		return nil, fmt.Errorf("failed to extract field values: %w", err)
	}

	queryStart := time.Now()
	var rows *sql.Rows

	if p.preparedStmt != nil {
		rows, err = p.preparedStmt.QueryContext(queryCtx, args...)
	} else {
		rows, err = p.db.QueryContext(queryCtx, p.config.Query, args...)
	}

	queryDuration := time.Since(queryStart)
	p.metrics.recordQuery(queryDuration, err == nil)

	if err != nil {
		logger.Error().
			Err(err).
			Dur("query_duration", queryDuration).
			Msg("enrichment query failed")
		return nil, fmt.Errorf("failed to execute enrichment query: %w", err)
	}
	defer rows.Close()

	enrichedRecord, err := p.addEnrichmentDataOptimized(record, rows)
	if err != nil {
		return nil, fmt.Errorf("failed to add enrichment data: %w", err)
	}

	logger.Trace().
		Dur("query_duration", queryDuration).
		Msg("record enrichment completed")

	return sdk.SingleRecord(enrichedRecord), nil
}

// Performance-optimized methods

func (p *SQLProcessor) extractFieldValuesOptimized(record opencdc.Record) ([]interface{}, error) {
	if len(p.fields) == 0 {
		return nil, nil
	}

	// Performance: Use pooled map
	data := p.recordPool.Get().(map[string]interface{})
	defer func() {
		// Clear map and return to pool
		for k := range data {
			delete(data, k)
		}
		p.recordPool.Put(data)
	}()

	if err := json.Unmarshal(record.Payload.After.Bytes(), &data); err != nil {
		return nil, fmt.Errorf("failed to parse record data: %w", err)
	}

	args := make([]interface{}, len(p.fields))
	for i, field := range p.fields {
		if value, exists := data[field]; exists {
			args[i] = value
		} else {
			args[i] = nil
		}
	}

	return args, nil
}

func (p *SQLProcessor) applyQueryResultsOptimized(record opencdc.Record, rows *sql.Rows) (opencdc.Record, error) {
	columns, err := rows.Columns()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to get columns: %w", err)
	}

	// Performance: Use pooled map
	data := p.recordPool.Get().(map[string]interface{})
	defer func() {
		for k := range data {
			delete(data, k)
		}
		p.recordPool.Put(data)
	}()

	if err := json.Unmarshal(record.Payload.After.Bytes(), &data); err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to parse record data: %w", err)
	}

	// Process first row only for transform mode
	if rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to scan row: %w", err)
		}

		// Update data with query results
		for i, column := range columns {
			data[column] = values[i]
		}
	}

	// Performance: Efficient marshaling
	newPayload, err := json.Marshal(data)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to marshal updated data: %w", err)
	}

	newRecord := record
	newRecord.Payload.After = opencdc.RawData(newPayload)

	return newRecord, nil
}

func (p *SQLProcessor) addEnrichmentDataOptimized(record opencdc.Record, rows *sql.Rows) (opencdc.Record, error) {
	columns, err := rows.Columns()
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to get columns: %w", err)
	}

	// Performance: Use pooled map
	data := p.recordPool.Get().(map[string]interface{})
	defer func() {
		for k := range data {
			delete(data, k)
		}
		p.recordPool.Put(data)
	}()

	if err := json.Unmarshal(record.Payload.After.Bytes(), &data); err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to parse record data: %w", err)
	}

	// Collect query results efficiently
	var enrichmentData []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return opencdc.Record{}, fmt.Errorf("failed to scan row: %w", err)
		}

		rowData := make(map[string]interface{})
		for i, column := range columns {
			rowData[column] = values[i]
		}
		enrichmentData = append(enrichmentData, rowData)
	}

	// Add enrichment data to record
	if len(enrichmentData) == 1 {
		data[p.config.OutputField] = enrichmentData[0]
	} else {
		data[p.config.OutputField] = enrichmentData
	}

	newPayload, err := json.Marshal(data)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("failed to marshal enriched data: %w", err)
	}

	newRecord := record
	newRecord.Payload.After = opencdc.RawData(newPayload)

	return newRecord, nil
}

// In-memory processing (optimized versions)

func (p *SQLProcessor) transformInMemory(record opencdc.Record) (sdk.ProcessedRecord, error) {
	// Performance: Use pooled map
	data := p.recordPool.Get().(map[string]interface{})
	defer func() {
		for k := range data {
			delete(data, k)
		}
		p.recordPool.Put(data)
	}()

	if err := json.Unmarshal(record.Payload.After.Bytes(), &data); err != nil {
		return nil, fmt.Errorf("failed to parse record data: %w", err)
	}

	transformedData, err := p.applySQLTransformationsOptimized(data)
	if err != nil {
		return nil, fmt.Errorf("failed to apply transformations: %w", err)
	}

	newPayload, err := json.Marshal(transformedData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal transformed data: %w", err)
	}

	newRecord := record
	newRecord.Payload.After = opencdc.RawData(newPayload)

	return sdk.SingleRecord(newRecord), nil
}

func (p *SQLProcessor) filterInMemoryOptimized(record opencdc.Record) (bool, error) {
	// Performance: Use pooled map
	data := p.recordPool.Get().(map[string]interface{})
	defer func() {
		for k := range data {
			delete(data, k)
		}
		p.recordPool.Put(data)
	}()

	if err := json.Unmarshal(record.Payload.After.Bytes(), &data); err != nil {
		return false, fmt.Errorf("failed to parse record data: %w", err)
	}

	// Simple WHERE clause evaluation (optimized)
	query := strings.ToUpper(strings.TrimSpace(p.config.Query))
	if whereIndex := strings.Index(query, "WHERE"); whereIndex != -1 {
		condition := strings.TrimSpace(query[whereIndex+5:])
		return p.evaluateConditionOptimized(condition, data)
	}

	return true, nil
}

func (p *SQLProcessor) applySQLTransformationsOptimized(data map[string]interface{}) (map[string]interface{}, error) {
	query := strings.ToUpper(strings.TrimSpace(p.config.Query))

	if strings.HasPrefix(query, "SELECT") {
		return p.processSelectQueryOptimized(query, data)
	}

	if strings.HasPrefix(query, "UPDATE") {
		return p.processUpdateQueryOptimized(query, data)
	}

	return data, nil
}

func (p *SQLProcessor) processSelectQueryOptimized(query string, data map[string]interface{}) (map[string]interface{}, error) {
	selectPart := strings.TrimPrefix(query, "SELECT ")
	if fromIndex := strings.Index(selectPart, " FROM"); fromIndex != -1 {
		selectPart = selectPart[:fromIndex]
	}

	result := make(map[string]interface{})

	if strings.TrimSpace(selectPart) == "*" {
		return data, nil
	}

	// Performance: Reuse string builder
	p.stringBuilder.Reset()

	fields := strings.Split(selectPart, ",")
	for _, field := range fields {
		field = strings.TrimSpace(field)

		if strings.Contains(field, " AS ") {
			parts := strings.Split(field, " AS ")
			if len(parts) == 2 {
				expr := strings.TrimSpace(parts[0])
				alias := strings.TrimSpace(parts[1])

				value, err := p.evaluateExpressionOptimized(expr, data)
				if err != nil {
					return nil, fmt.Errorf("failed to evaluate expression %s: %w", expr, err)
				}
				result[alias] = value
				continue
			}
		}

		if value, exists := data[field]; exists {
			result[field] = value
		}
	}

	return result, nil
}

func (p *SQLProcessor) processUpdateQueryOptimized(query string, data map[string]interface{}) (map[string]interface{}, error) {
	setPart := ""
	if setIndex := strings.Index(query, " SET "); setIndex != -1 {
		setPart = query[setIndex+5:]
		if whereIndex := strings.Index(setPart, " WHERE"); whereIndex != -1 {
			setPart = setPart[:whereIndex]
		}
	}

	assignments := strings.Split(setPart, ",")
	for _, assignment := range assignments {
		parts := strings.Split(assignment, "=")
		if len(parts) == 2 {
			field := strings.TrimSpace(parts[0])
			expr := strings.TrimSpace(parts[1])

			value, err := p.evaluateExpressionOptimized(expr, data)
			if err != nil {
				return nil, fmt.Errorf("failed to evaluate expression %s: %w", expr, err)
			}
			data[field] = value
		}
	}

	return data, nil
}

func (p *SQLProcessor) evaluateExpressionOptimized(expr string, data map[string]interface{}) (interface{}, error) {
	expr = strings.TrimSpace(expr)

	// Handle string literals
	if strings.HasPrefix(expr, "'") && strings.HasSuffix(expr, "'") {
		return expr[1 : len(expr)-1], nil
	}

	// Handle numeric literals
	if num, err := strconv.ParseFloat(expr, 64); err == nil {
		return num, nil
	}

	// Handle field references
	if value, exists := data[expr]; exists {
		return value, nil
	}

	// Handle concatenation (performance optimized)
	if strings.Contains(expr, "||") {
		p.stringBuilder.Reset()
		parts := strings.Split(expr, "||")
		for _, part := range parts {
			part = strings.TrimSpace(part)
			val, err := p.evaluateExpressionOptimized(part, data)
			if err != nil {
				return nil, err
			}
			p.stringBuilder.WriteString(fmt.Sprintf("%v", val))
		}
		return p.stringBuilder.String(), nil
	}

	// Handle arithmetic
	if strings.Contains(expr, "+") && !strings.Contains(expr, "'") {
		parts := strings.Split(expr, "+")
		if len(parts) == 2 {
			val1, err1 := p.evaluateExpressionOptimized(strings.TrimSpace(parts[0]), data)
			val2, err2 := p.evaluateExpressionOptimized(strings.TrimSpace(parts[1]), data)
			if err1 == nil && err2 == nil {
				if num1, ok1 := val1.(float64); ok1 {
					if num2, ok2 := val2.(float64); ok2 {
						return num1 + num2, nil
					}
				}
			}
		}
	}

	return expr, nil
}

func (p *SQLProcessor) evaluateConditionOptimized(condition string, data map[string]interface{}) (bool, error) {
	if strings.Contains(condition, "=") {
		parts := strings.Split(condition, "=")
		if len(parts) == 2 {
			left := strings.TrimSpace(parts[0])
			right := strings.TrimSpace(parts[1])

			leftVal, err := p.evaluateExpressionOptimized(left, data)
			if err != nil {
				return false, err
			}

			rightVal, err := p.evaluateExpressionOptimized(right, data)
			if err != nil {
				return false, err
			}

			return fmt.Sprintf("%v", leftVal) == fmt.Sprintf("%v", rightVal), nil
		}
	}

	return true, nil
}

func (p *SQLProcessor) Teardown(ctx context.Context) error {
	logger := sdk.Logger(ctx)

	// Log final metrics
	processed, queries, errors, totalDuration := p.metrics.getStats()
	logger.Info().
		Int64("records_processed", processed).
		Int64("queries_executed", queries).
		Int64("query_errors", errors).
		Dur("total_query_duration", totalDuration).
		Msg("SQL processor teardown - final metrics")

	// Close prepared statement
	if p.preparedStmt != nil {
		if err := p.preparedStmt.Close(); err != nil {
			logger.Warn().Err(err).Msg("failed to close prepared statement")
		}
	}

	// Close database connection
	if p.db != nil {
		if err := p.db.Close(); err != nil {
			logger.Error().Err(err).Msg("failed to close database connection")
			return err
		}
		logger.Info().Msg("database connection closed successfully")
	}

	return nil
}

// Security and validation helpers

func (p *SQLProcessor) validateConfig() error {
	// Validate query length
	if len(p.config.Query) > maxQueryLength {
		return fmt.Errorf("query too long (max %d chars): %d", maxQueryLength, len(p.config.Query))
	}

	// Security: Check for dangerous SQL keywords in transform mode
	if p.config.Mode == "transform" && p.config.ConnectionString == "" {
		dangerousKeywords := []string{"DROP", "DELETE", "INSERT", "UPDATE", "TRUNCATE", "ALTER", "CREATE"}
		upperQuery := strings.ToUpper(p.config.Query)
		for _, keyword := range dangerousKeywords {
			if strings.Contains(upperQuery, keyword) {
				return fmt.Errorf("potentially dangerous SQL keyword '%s' detected in transform mode", keyword)
			}
		}
	}

	// Validate timeouts
	if p.config.QueryTimeout < time.Second {
		return fmt.Errorf("query timeout too short (min 1s): %v", p.config.QueryTimeout)
	}

	if p.config.QueryTimeout > 5*time.Minute {
		return fmt.Errorf("query timeout too long (max 5m): %v", p.config.QueryTimeout)
	}

	return nil
}

func isValidFieldName(field string) bool {
	if len(field) == 0 || len(field) > 64 {
		return false
	}

	// Security: Allow only alphanumeric, underscore, and dot
	for _, r := range field {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '.') {
			return false
		}
	}

	return true
}

func redactConnectionString(connStr string) string {
	// Security: Redact passwords and sensitive info
	sensitive := []string{"password=", "pwd=", "secret=", "token="}
	result := connStr

	for _, keyword := range sensitive {
		if idx := strings.Index(strings.ToLower(result), keyword); idx != -1 {
			start := idx + len(keyword)
			end := strings.IndexAny(result[start:], " ;&")
			if end == -1 {
				end = len(result) - start
			}

			redacted := result[:start] + "***" + result[start+end:]
			result = redacted
		}
	}

	return result
}

// NewProcessor creates a new instance of the SQL processor
func NewProcessor() sdk.Processor {
	return &SQLProcessor{}
}
