//go:build wasm

// Package sql provides a WASM-compatible SQL processor for Conduit that enables
// SQL-based transformations and filtering of records using in-memory operations.
package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
)


const (
	// Performance constants
	defaultBatchSize = 100

	// Security constants - prevent DoS and injection attacks
	maxQueryLength = 10000
	maxFieldCount  = 50
	maxPayloadSize = 1024 * 1024 // 1MB limit per record
)

// Processor implements a WASM-compatible SQL processor for Conduit.
// It provides SQL-like transformations and filtering capabilities using
// in-memory operations without requiring database connectivity.
type Processor struct {
	sdk.UnimplementedProcessor

	config ProcessorConfig

	// Performance optimizations
	recordPool    sync.Pool
	stringBuilder strings.Builder

	// Monitoring and metrics
	mu      sync.RWMutex
	metrics ProcessorMetrics
}


// ProcessorMetrics tracks processor performance and health metrics.
type ProcessorMetrics struct {
	recordsProcessed int64         // Total records processed
	operationsCount  int64         // Total SQL operations executed
	operationErrors  int64         // Number of operation errors
	totalDuration    time.Duration // Cumulative processing time
	mu               sync.RWMutex  // Protects metric updates
}

// recordOperation updates operation metrics in a thread-safe manner.
func (m *ProcessorMetrics) recordOperation(duration time.Duration, success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.operationsCount++
	m.totalDuration += duration
	if !success {
		m.operationErrors++
	}
}

// recordProcessed updates the processed record count.
func (m *ProcessorMetrics) recordProcessed(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recordsProcessed += int64(count)
}

// getStats returns current metrics in a thread-safe manner.
func (m *ProcessorMetrics) getStats() (int64, int64, int64, time.Duration) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.recordsProcessed, m.operationsCount, m.operationErrors, m.totalDuration
}

// Specification returns the processor specification following Conduit SDK conventions.
// This method is required by the Conduit processor interface.
func (p *Processor) Specification() (sdk.Specification, error) {
	return sdk.Specification{
		Name:    "sql.threshold",
		Summary: "Filter records based on numeric field threshold (WASM compatible)",
		Description: `A simple processor that filters records based on a numeric field value.
Records where the specified field's value exceeds the configured threshold will be passed through,
while others will be filtered out. This processor is optimized for WebAssembly environments.

Features:
- Numeric threshold comparison for efficient filtering
- High-performance in-memory processing
- Simple configuration with just two parameters

WASM Limitations:
- No database connectivity
- No network operations or external API calls
- In-memory processing only`,
		Version:    "v1.0.0",
		Author:     "Conduit Contributors",
		Parameters: ProcessorConfig{}.Parameters(),
	}, nil
}

// Configure validates and initializes the processor configuration.
// This method is called once during processor initialization.
func (p *Processor) Configure(ctx context.Context, cfg config.Config) error {
	logger := sdk.Logger(ctx)
	logger.Debug().Msg("configuring SQL processor")

	// Decode configuration using SDK conventions
	if err := cfg.DecodeInto(&p.config); err != nil {
		logger.Error().Err(err).Msg("failed to decode configuration")
		return fmt.Errorf("failed to decode configuration: %w", err)
	}

	// Validate configuration
	if err := p.validateConfig(); err != nil {
		logger.Error().Err(err).Msg("configuration validation failed")
		return fmt.Errorf("configuration validation failed: %w", err)
	}

	// Initialize performance optimizations
	p.recordPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]interface{})
		},
	}

	logger.Info().
		Str("field", p.config.Field).
		Int("threshold", p.config.Threshold).
		Msg("SQL processor configured successfully")

	return nil
}

// Open initializes the processor for processing records.
// This method is called after Configure and before any Process calls.
func (p *Processor) Open(ctx context.Context) error {
	logger := sdk.Logger(ctx)
	logger.Info().
		Str("version", "v1.0.0").
		Str("field", p.config.Field).
		Int("threshold", p.config.Threshold).
		Msg("SQL processor opened - filtering records where field value exceeds threshold")
	return nil
}

// Process filters records based on whether the configured field's value exceeds the threshold.
// This is the main processing method called by Conduit for each batch of records.
func (p *Processor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	logger := sdk.Logger(ctx)
	logger.Debug().Int("record_count", len(records)).Msg("processing batch")

	start := time.Now()
	result := make([]sdk.ProcessedRecord, len(records))

	successCount := 0
	errorCount := 0

	// Process each record individually
	for i, record := range records {
		recordStart := time.Now()

		// Parse record payload
		var data map[string]interface{}
		if err := json.Unmarshal(record.Payload.After.Bytes(), &data); err != nil {
			logger.Debug().
				Err(err).
				Int("record_index", i).
				Str("record_position", string(record.Position)).
				Msg("failed to parse record data")

			result[i] = sdk.ErrorRecord{Error: err}
			errorCount++
			p.metrics.recordOperation(time.Since(recordStart), false)
			continue
		}

		// Check if the field exists and compare with threshold
		if val, ok := data[p.config.Field]; ok {
			// Convert value to number for comparison
			if num, ok := p.toNumber(val); ok {
				if num > float64(p.config.Threshold) {
					result[i] = sdk.SingleRecord(record)
				} else {
					result[i] = sdk.FilterRecord{}
				}
				successCount++
				p.metrics.recordOperation(time.Since(recordStart), true)
				continue
			}
		}

		// If field doesn't exist or value isn't numeric, filter the record
		result[i] = sdk.FilterRecord{}
		successCount++
		p.metrics.recordOperation(time.Since(recordStart), true)
	}

	processingDuration := time.Since(start)
	p.metrics.recordProcessed(len(records))

	// Log batch processing summary
	logger.Debug().
		Int("total_records", len(records)).
		Int("successful", successCount).
		Int("errors", errorCount).
		Dur("total_duration", processingDuration).
		Dur("avg_per_record", processingDuration/time.Duration(len(records))).
		Msg("batch processing completed")

	return result
}


// toNumber attempts to convert a value to a float64 for numeric operations.
func (p *Processor) toNumber(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case string:
		if num, err := strconv.ParseFloat(v, 64); err == nil {
			return num, true
		}
	}
	return 0, false
}

// Teardown cleans up processor resources and logs final metrics.
// This method is called when the processor is being shut down.
func (p *Processor) Teardown(ctx context.Context) error {
	logger := sdk.Logger(ctx)

	// Log final processing metrics
	processed, operations, errors, totalDuration := p.metrics.getStats()
	logger.Info().
		Int64("records_processed", processed).
		Int64("operations_executed", operations).
		Int64("operation_errors", errors).
		Dur("total_operation_duration", totalDuration).
		Float64("error_rate", float64(errors)/float64(operations)*100).
		Msg("SQL processor teardown - final metrics")

	return nil
}

// validateConfig validates the processor configuration.
func (p *Processor) validateConfig() error {
	// Validate Field is specified
	if p.config.Field == "" {
		return fmt.Errorf("field cannot be empty")
	}

	// Validate field name for security
	if !p.isValidFieldName(p.config.Field) {
		return fmt.Errorf("invalid field name: %s", p.config.Field)
	}

	// Threshold validation - ensure it's not negative
	if p.config.Threshold < 0 {
		return fmt.Errorf("threshold cannot be negative, got %d", p.config.Threshold)
	}

	return nil
}

// isValidFieldName validates field names for security (prevents injection attacks).
func (p *Processor) isValidFieldName(field string) bool {
	if len(field) == 0 || len(field) > 64 {
		return false
	}

	// Allow only alphanumeric characters, underscores, and dots
	for _, r := range field {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '.') {
			return false
		}
	}

	return true
}

// NewProcessor creates and returns a new instance of the SQL processor.
// This function is called by the Conduit framework to instantiate the processor.
func NewProcessor() sdk.Processor {
	return &Processor{}
}
