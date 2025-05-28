//go:build wasm

package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
)

func TestSQLProcessor_Configure(t *testing.T) {
	tests := []struct {
		name    string
		config  map[string]string
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid transform config",
			config: map[string]string{
				"query":  "SELECT name, UPPER(name) as upper_name",
				"mode":   "transform",
				"driver": "postgres",
			},
			wantErr: false,
		},
		{
			name: "valid filter config with fields",
			config: map[string]string{
				"query":  "SELECT age > 18 as is_adult WHERE id = $1",
				"mode":   "filter",
				"fields": "id",
				"driver": "mysql",
			},
			wantErr: false,
		},
		{
			name: "valid enrich config with connection",
			config: map[string]string{
				"query":            "SELECT email, phone FROM users WHERE id = $1",
				"mode":             "enrich",
				"connectionString": "postgres://user:pass@localhost/db",
				"fields":           "user_id",
				"outputField":      "user_data",
			},
			wantErr: false,
		},
		{
			name: "missing required query",
			config: map[string]string{
				"mode": "transform",
			},
			wantErr: true,
			errMsg:  "query",
		},
		{
			name: "invalid mode",
			config: map[string]string{
				"query": "SELECT * FROM table",
				"mode":  "invalid",
			},
			wantErr: true,
			errMsg:  "mode",
		},
		{
			name: "invalid driver",
			config: map[string]string{
				"query":  "SELECT *",
				"mode":   "transform",
				"driver": "invalid",
			},
			wantErr: true,
			errMsg:  "driver",
		},
		{
			name: "query too long",
			config: map[string]string{
				"query": strings.Repeat("SELECT * FROM table WHERE condition = 'value' AND ", 500),
				"mode":  "transform",
			},
			wantErr: true,
			errMsg:  "too long",
		},
		{
			name: "too many fields",
			config: map[string]string{
				"query":  "SELECT *",
				"mode":   "transform",
				"fields": strings.Repeat("field,", 60), // Over 50 fields
			},
			wantErr: true,
			errMsg:  "too many fields",
		},
		{
			name: "invalid field name",
			config: map[string]string{
				"query":  "SELECT *",
				"mode":   "transform",
				"fields": "valid_field,invalid-field!",
			},
			wantErr: true,
			errMsg:  "invalid field name",
		},
		{
			name: "dangerous SQL in transform mode",
			config: map[string]string{
				"query": "DROP TABLE users",
				"mode":  "transform",
			},
			wantErr: true,
			errMsg:  "dangerous SQL keyword",
		},
		{
			name: "query timeout too short",
			config: map[string]string{
				"query":        "SELECT *",
				"mode":         "transform",
				"queryTimeout": "500ms",
			},
			wantErr: true,
			errMsg:  "timeout too short",
		},
		{
			name: "query timeout too long",
			config: map[string]string{
				"query":        "SELECT *",
				"mode":         "transform",
				"queryTimeout": "10m",
			},
			wantErr: true,
			errMsg:  "timeout too long",
		},
		{
			name: "valid performance config",
			config: map[string]string{
				"query":           "SELECT *",
				"mode":            "transform",
				"maxOpenConns":    "20",
				"maxIdleConns":    "10",
				"connMaxLifetime": "1h",
				"queryTimeout":    "1m",
				"batchSize":       "200",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &SQLProcessor{}
			cfg := config.Config{}

			// Convert string map to proper config
			for k, v := range tt.config {
				cfg[k] = v
			}

			err := p.Configure(context.Background(), cfg)

			if tt.wantErr {
				if err == nil {
					t.Errorf("Configure() expected error but got none")
					return
				}
				if tt.errMsg != "" && !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.errMsg)) {
					t.Errorf("Configure() error = %v, want error containing %v", err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("Configure() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestSQLProcessor_TransformInMemory(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		input    map[string]interface{}
		expected map[string]interface{}
		wantErr  bool
	}{
		{
			name:  "select all fields",
			query: "SELECT *",
			input: map[string]interface{}{
				"id":   1,
				"name": "John",
				"age":  30,
			},
			expected: map[string]interface{}{
				"id":   1,
				"name": "John",
				"age":  30,
			},
			wantErr: false,
		},
		{
			name:  "select specific fields",
			query: "SELECT name, age",
			input: map[string]interface{}{
				"id":   1,
				"name": "John",
				"age":  30,
			},
			expected: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			wantErr: false,
		},
		{
			name:  "select with alias",
			query: "SELECT name AS full_name, age",
			input: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			expected: map[string]interface{}{
				"full_name": "John",
				"age":       30,
			},
			wantErr: false,
		},
		{
			name:  "update field value",
			query: "UPDATE SET age = 31",
			input: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			expected: map[string]interface{}{
				"name": "John",
				"age":  31.0,
			},
			wantErr: false,
		},
		{
			name:  "string concatenation",
			query: "SELECT first_name || ' ' || last_name AS full_name",
			input: map[string]interface{}{
				"first_name": "John",
				"last_name":  "Doe",
			},
			expected: map[string]interface{}{
				"full_name": "John Doe",
			},
			wantErr: false,
		},
		{
			name:  "numeric calculation",
			query: "SELECT age, age + 1 AS next_age",
			input: map[string]interface{}{
				"age": 25.0,
			},
			expected: map[string]interface{}{
				"age":      25.0,
				"next_age": 26.0,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &SQLProcessor{
				config: ProcessorConfig{
					Query: tt.query,
					Mode:  "transform",
				},
			}

			// Initialize record pool
			p.recordPool.New = func() interface{} {
				return make(map[string]interface{})
			}

			// Create test record
			inputData, _ := json.Marshal(tt.input)
			record := opencdc.Record{
				Payload: opencdc.Change{
					After: opencdc.RawData(inputData),
				},
			}

			result, err := p.transformInMemory(record)

			if tt.wantErr {
				if err == nil {
					t.Errorf("transformInMemory() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("transformInMemory() error = %v", err)
			}

			// Extract result data
			singleRecord, ok := result.(sdk.SingleRecord)
			if !ok {
				t.Fatalf("expected SingleRecord, got %T", result)
			}

			var resultData map[string]interface{}
			err = json.Unmarshal(singleRecord.Payload.After.Bytes(), &resultData)
			if err != nil {
				t.Fatalf("failed to unmarshal result: %v", err)
			}

			// Compare results
			if !equalMaps(resultData, tt.expected) {
				t.Errorf("transformInMemory() = %v, want %v", resultData, tt.expected)
			}
		})
	}
}

func TestSQLProcessor_FilterInMemory(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		input    map[string]interface{}
		expected bool
		wantErr  bool
	}{
		{
			name:  "simple equality filter - match",
			query: "SELECT * WHERE age = 30",
			input: map[string]interface{}{
				"name": "John",
				"age":  30.0,
			},
			expected: true,
			wantErr:  false,
		},
		{
			name:  "simple equality filter - no match",
			query: "SELECT * WHERE age = 25",
			input: map[string]interface{}{
				"name": "John",
				"age":  30.0,
			},
			expected: false,
			wantErr:  false,
		},
		{
			name:  "string equality filter - match",
			query: "SELECT * WHERE name = 'John'",
			input: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			expected: true,
			wantErr:  false,
		},
		{
			name:  "no where clause",
			query: "SELECT * FROM table",
			input: map[string]interface{}{
				"name": "John",
				"age":  30,
			},
			expected: true,
			wantErr:  false,
		},
		{
			name:  "field reference",
			query: "SELECT * WHERE status = 'active'",
			input: map[string]interface{}{
				"name":   "John",
				"status": "active",
			},
			expected: true,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &SQLProcessor{
				config: ProcessorConfig{
					Query: tt.query,
					Mode:  "filter",
				},
			}

			// Initialize record pool
			p.recordPool.New = func() interface{} {
				return make(map[string]interface{})
			}

			// Create test record
			inputData, _ := json.Marshal(tt.input)
			record := opencdc.Record{
				Payload: opencdc.Change{
					After: opencdc.RawData(inputData),
				},
			}

			result, err := p.filterInMemoryOptimized(record)

			if tt.wantErr {
				if err == nil {
					t.Errorf("filterInMemoryOptimized() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("filterInMemoryOptimized() error = %v", err)
			}

			if result != tt.expected {
				t.Errorf("filterInMemoryOptimized() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSQLProcessor_EvaluateExpression(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		data     map[string]interface{}
		expected interface{}
		wantErr  bool
	}{
		{
			name:     "string literal",
			expr:     "'hello'",
			data:     map[string]interface{}{},
			expected: "hello",
			wantErr:  false,
		},
		{
			name:     "numeric literal",
			expr:     "42",
			data:     map[string]interface{}{},
			expected: 42.0,
			wantErr:  false,
		},
		{
			name: "field reference",
			expr: "name",
			data: map[string]interface{}{
				"name": "John",
			},
			expected: "John",
			wantErr:  false,
		},
		{
			name: "string concatenation",
			expr: "first_name || last_name",
			data: map[string]interface{}{
				"first_name": "John",
				"last_name":  "Doe",
			},
			expected: "JohnDoe",
			wantErr:  false,
		},
		{
			name: "numeric addition",
			expr: "age + 1",
			data: map[string]interface{}{
				"age": 30.0,
			},
			expected: 31.0,
			wantErr:  false,
		},
		{
			name: "complex concatenation with spaces",
			expr: "first_name || ' ' || last_name",
			data: map[string]interface{}{
				"first_name": "John",
				"last_name":  "Doe",
			},
			expected: "John Doe",
			wantErr:  false,
		},
		{
			name:     "decimal number",
			expr:     "3.14",
			data:     map[string]interface{}{},
			expected: 3.14,
			wantErr:  false,
		},
		{
			name: "missing field reference",
			expr: "missing_field",
			data: map[string]interface{}{
				"name": "John",
			},
			expected: "missing_field", // Returns the expression itself if field not found
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &SQLProcessor{}

			result, err := p.evaluateExpressionOptimized(tt.expr, tt.data)

			if tt.wantErr {
				if err == nil {
					t.Errorf("evaluateExpressionOptimized() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("evaluateExpressionOptimized() error = %v", err)
			}

			if result != tt.expected {
				t.Errorf("evaluateExpressionOptimized() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSQLProcessor_ExtractFieldValues(t *testing.T) {
	tests := []struct {
		name     string
		fields   []string
		input    map[string]interface{}
		expected []interface{}
		wantErr  bool
	}{
		{
			name:   "extract single field",
			fields: []string{"id"},
			input: map[string]interface{}{
				"id":   1,
				"name": "John",
			},
			expected: []interface{}{1},
			wantErr:  false,
		},
		{
			name:   "extract multiple fields",
			fields: []string{"id", "name", "age"},
			input: map[string]interface{}{
				"id":   1,
				"name": "John",
				"age":  30,
			},
			expected: []interface{}{1, "John", 30},
			wantErr:  false,
		},
		{
			name:   "extract missing field",
			fields: []string{"missing"},
			input: map[string]interface{}{
				"id": 1,
			},
			expected: []interface{}{nil},
			wantErr:  false,
		},
		{
			name:     "no fields specified",
			fields:   []string{},
			input:    map[string]interface{}{"id": 1},
			expected: nil,
			wantErr:  false,
		},
		{
			name:   "extract with different data types",
			fields: []string{"string", "number", "boolean", "null"},
			input: map[string]interface{}{
				"string":  "test",
				"number":  42.5,
				"boolean": true,
				"null":    nil,
			},
			expected: []interface{}{"test", 42.5, true, nil},
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &SQLProcessor{
				fields: tt.fields,
			}

			// Initialize record pool
			p.recordPool.New = func() interface{} {
				return make(map[string]interface{})
			}

			// Create test record
			inputData, _ := json.Marshal(tt.input)
			record := opencdc.Record{
				Payload: opencdc.Change{
					After: opencdc.RawData(inputData),
				},
			}

			result, err := p.extractFieldValuesOptimized(record)

			if tt.wantErr {
				if err == nil {
					t.Errorf("extractFieldValuesOptimized() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("extractFieldValuesOptimized() error = %v", err)
			}

			if !equalSlices(result, tt.expected) {
				t.Errorf("extractFieldValuesOptimized() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestSQLProcessor_Process(t *testing.T) {
	tests := []struct {
		name           string
		config         ProcessorConfig
		input          []opencdc.Record
		expectedCount  int
		expectedErrors int
		expectedType   string // "SingleRecord", "FilterRecord", "ErrorRecord"
	}{
		{
			name: "transform single record",
			config: ProcessorConfig{
				Query: "SELECT *",
				Mode:  "transform",
			},
			input: []opencdc.Record{
				createTestRecord(map[string]interface{}{"id": 1, "name": "John"}),
			},
			expectedCount:  1,
			expectedErrors: 0,
			expectedType:   "SingleRecord",
		},
		{
			name: "filter records - pass",
			config: ProcessorConfig{
				Query: "SELECT * WHERE age = 30",
				Mode:  "filter",
			},
			input: []opencdc.Record{
				createTestRecord(map[string]interface{}{"age": 30.0}),
			},
			expectedCount:  1,
			expectedErrors: 0,
			expectedType:   "SingleRecord",
		},
		{
			name: "filter records - block",
			config: ProcessorConfig{
				Query: "SELECT * WHERE age = 30",
				Mode:  "filter",
			},
			input: []opencdc.Record{
				createTestRecord(map[string]interface{}{"age": 25.0}),
			},
			expectedCount:  1,
			expectedErrors: 0,
			expectedType:   "FilterRecord",
		},
		{
			name: "invalid mode",
			config: ProcessorConfig{
				Query: "SELECT *",
				Mode:  "invalid",
			},
			input: []opencdc.Record{
				createTestRecord(map[string]interface{}{"id": 1}),
			},
			expectedCount:  1,
			expectedErrors: 1,
			expectedType:   "ErrorRecord",
		},
		{
			name: "batch processing",
			config: ProcessorConfig{
				Query:     "SELECT name, UPPER(name) AS upper_name",
				Mode:      "transform",
				BatchSize: 2,
			},
			input: []opencdc.Record{
				createTestRecord(map[string]interface{}{"name": "John"}),
				createTestRecord(map[string]interface{}{"name": "Jane"}),
				createTestRecord(map[string]interface{}{"name": "Bob"}),
			},
			expectedCount:  3,
			expectedErrors: 0,
			expectedType:   "SingleRecord",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &SQLProcessor{
				config: tt.config,
			}

			// Initialize record pool
			p.recordPool.New = func() interface{} {
				return make(map[string]interface{})
			}

			results := p.Process(context.Background(), tt.input)

			if len(results) != tt.expectedCount {
				t.Errorf("Process() returned %d results, want %d", len(results), tt.expectedCount)
			}

			errorCount := 0
			for _, result := range results {
				if _, isError := result.(sdk.ErrorRecord); isError {
					errorCount++
				}
			}

			if errorCount != tt.expectedErrors {
				t.Errorf("Process() had %d errors, want %d", errorCount, tt.expectedErrors)
			}

			// Check specific result types for non-error cases
			if tt.expectedErrors == 0 && len(results) > 0 {
				switch tt.expectedType {
				case "SingleRecord":
					if _, ok := results[0].(sdk.SingleRecord); !ok {
						t.Errorf("Process() expected SingleRecord, got %T", results[0])
					}
				case "FilterRecord":
					if _, ok := results[0].(sdk.FilterRecord); !ok {
						t.Errorf("Process() expected FilterRecord, got %T", results[0])
					}
				}
			}
		})
	}
}

func TestSQLProcessor_Specification(t *testing.T) {
	p := &SQLProcessor{}

	spec, err := p.Specification()
	if err != nil {
		t.Fatalf("Specification() error = %v", err)
	}

	// Validate specification fields
	if spec.Name == "" {
		t.Error("Specification() Name should not be empty")
	}
	if spec.Summary == "" {
		t.Error("Specification() Summary should not be empty")
	}
	if spec.Description == "" {
		t.Error("Specification() Description should not be empty")
	}
	if spec.Version == "" {
		t.Error("Specification() Version should not be empty")
	}
	if spec.Author == "" {
		t.Error("Specification() Author should not be empty")
	}

	// Check required parameters
	requiredParams := []string{"query", "mode", "driver"}
	for _, param := range requiredParams {
		if _, exists := spec.Parameters[param]; !exists {
			t.Errorf("Specification() missing required parameter: %s", param)
		}
	}
}

func TestSQLProcessor_SecurityValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  ProcessorConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "dangerous DROP statement",
			config: ProcessorConfig{
				Query: "DROP TABLE users",
				Mode:  "transform",
			},
			wantErr: true,
			errMsg:  "dangerous SQL keyword",
		},
		{
			name: "dangerous DELETE statement",
			config: ProcessorConfig{
				Query: "DELETE FROM users WHERE id = 1",
				Mode:  "transform",
			},
			wantErr: true,
			errMsg:  "dangerous SQL keyword",
		},
		{
			name: "safe SELECT statement",
			config: ProcessorConfig{
				Query: "SELECT name, email FROM users",
				Mode:  "transform",
			},
			wantErr: false,
		},
		{
			name: "UPDATE allowed with database connection",
			config: ProcessorConfig{
				Query:            "UPDATE users SET last_login = NOW() WHERE id = $1",
				Mode:             "enrich",
				ConnectionString: "postgres://user:pass@localhost/db",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &SQLProcessor{
				config: tt.config,
			}

			err := p.validateConfig()

			if tt.wantErr {
				if err == nil {
					t.Errorf("validateConfig() expected error but got none")
					return
				}
				if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.errMsg)) {
					t.Errorf("validateConfig() error = %v, want error containing %v", err, tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("validateConfig() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestSQLProcessor_FieldValidation(t *testing.T) {
	tests := []struct {
		name      string
		fieldName string
		valid     bool
	}{
		{"valid simple field", "user_id", true},
		{"valid with dots", "user.id", true},
		{"valid alphanumeric", "field123", true},
		{"invalid with dash", "user-id", false},
		{"invalid with space", "user id", false},
		{"invalid with special chars", "user@id", false},
		{"invalid empty", "", false},
		{"invalid too long", strings.Repeat("a", 65), false},
		{"valid with underscores", "user_profile_data", true},
		{"valid mixed case", "UserID", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidFieldName(tt.fieldName)
			if result != tt.valid {
				t.Errorf("isValidFieldName(%q) = %v, want %v", tt.fieldName, result, tt.valid)
			}
		})
	}
}

func TestSQLProcessor_RedactConnectionString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "postgres connection",
			input:    "postgres://user:secret123@localhost:5432/mydb",
			expected: "postgres://user:***@localhost:5432/mydb",
		},
		{
			name:     "mysql connection",
			input:    "user:secret123@tcp(localhost:3306)/mydb",
			expected: "user:***@tcp(localhost:3306)/mydb",
		},
		{
			name:     "connection with password param",
			input:    "host=localhost user=myuser password=secret123 dbname=mydb",
			expected: "host=localhost user=myuser password=*** dbname=mydb",
		},
		{
			name:     "no sensitive info",
			input:    "host=localhost user=myuser dbname=mydb",
			expected: "host=localhost user=myuser dbname=mydb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := redactConnectionString(tt.input)
			if result != tt.expected {
				t.Errorf("redactConnectionString(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// Performance and load tests

func TestSQLProcessor_LargePayload(t *testing.T) {
	p := &SQLProcessor{
		config: ProcessorConfig{
			Query: "SELECT *",
			Mode:  "transform",
		},
	}

	// Initialize record pool
	p.recordPool.New = func() interface{} {
		return make(map[string]interface{})
	}

	// Create large payload (but under the 1MB limit)
	largeData := make(map[string]interface{})
	for i := 0; i < 1000; i++ {
		largeData[fmt.Sprintf("field_%d", i)] = fmt.Sprintf("value_%d", i)
	}

	record := createTestRecord(largeData)
	results := p.Process(context.Background(), []opencdc.Record{record})

	if len(results) != 1 {
		t.Errorf("Process() returned %d results, want 1", len(results))
	}

	if _, isError := results[0].(sdk.ErrorRecord); isError {
		t.Errorf("Process() returned error for large but valid payload")
	}
}

func TestSQLProcessor_PayloadTooLarge(t *testing.T) {
	p := &SQLProcessor{
		config: ProcessorConfig{
			Query: "SELECT *",
			Mode:  "transform",
		},
	}

	// Create payload larger than 1MB
	largeString := strings.Repeat("x", 1024*1024+1)
	largeData := map[string]interface{}{
		"large_field": largeString,
	}

	record := createTestRecord(largeData)
	results := p.Process(context.Background(), []opencdc.Record{record})

	if len(results) != 1 {
		t.Errorf("Process() returned %d results, want 1", len(results))
	}

	if _, isError := results[0].(sdk.ErrorRecord); !isError {
		t.Errorf("Process() should return error for payload larger than 1MB")
	}
}

// Benchmark tests

func BenchmarkSQLProcessor_Process(b *testing.B) {
	p := &SQLProcessor{
		config: ProcessorConfig{
			Query: "SELECT name, age",
			Mode:  "transform",
		},
	}

	// Initialize record pool
	p.recordPool.New = func() interface{} {
		return make(map[string]interface{})
	}

	record := createTestRecord(map[string]interface{}{
		"name": "John",
		"age":  30,
	})
	records := []opencdc.Record{record}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Process(context.Background(), records)
	}
}

func BenchmarkSQLProcessor_EvaluateExpression(b *testing.B) {
	p := &SQLProcessor{}
	data := map[string]interface{}{
		"first_name": "John",
		"last_name":  "Doe",
		"age":        30.0,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.evaluateExpressionOptimized("first_name || last_name", data)
	}
}

func BenchmarkSQLProcessor_BatchProcessing(b *testing.B) {
	p := &SQLProcessor{
		config: ProcessorConfig{
			Query:     "SELECT name, UPPER(name) AS upper_name",
			Mode:      "transform",
			BatchSize: 100,
		},
	}

	// Initialize record pool
	p.recordPool.New = func() interface{} {
		return make(map[string]interface{})
	}

	// Create batch of records
	records := make([]opencdc.Record, 100)
	for i := 0; i < 100; i++ {
		records[i] = createTestRecord(map[string]interface{}{
			"name": fmt.Sprintf("User%d", i),
			"id":   i,
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Process(context.Background(), records)
	}
}

func BenchmarkSQLProcessor_MemoryPooling(b *testing.B) {
	p := &SQLProcessor{
		config: ProcessorConfig{
			Query: "SELECT *",
			Mode:  "transform",
		},
	}

	// Initialize record pool
	p.recordPool.New = func() interface{} {
		return make(map[string]interface{})
	}

	record := createTestRecord(map[string]interface{}{
		"field1": "value1",
		"field2": "value2",
		"field3": 42,
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.extractFieldValuesOptimized(record)
	}
}

// Helper functions for testing

func createTestRecord(data map[string]interface{}) opencdc.Record {
	payload, _ := json.Marshal(data)
	return opencdc.Record{
		Position: opencdc.Position(fmt.Sprintf("test-position-%d", time.Now().UnixNano())),
		Payload: opencdc.Change{
			After: opencdc.RawData(payload),
		},
		Metadata: opencdc.Metadata{
			"test.source": "unit-test",
		},
	}
}

func equalMaps(a, b map[string]interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for key, valueA := range a {
		valueB, exists := b[key]
		if !exists {
			return false
		}
		if valueA != valueB {
			return false
		}
	}
	return true
}

func equalSlices(a, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
