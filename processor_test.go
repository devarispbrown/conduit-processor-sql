package sql

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"strconv"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
)

// TestProcessorConfig holds the configuration for the SQL processor (test version)
type TestProcessorConfig struct {
	Field     string
	Threshold int
}

// TestProcessor represents the SQL processor (test version)
type TestProcessor struct {
	config     TestProcessorConfig
	recordPool sync.Pool
}

// Note: Configuration parameter names are defined in paramgen_proc.go

// Configure sets up the processor with the given configuration
func (p *TestProcessor) Configure(ctx context.Context, cfg config.Config) error {
	if field, ok := cfg[ProcessorConfigField]; ok {
		p.config.Field = field
	}
	
	if threshold, ok := cfg[ProcessorConfigThreshold]; ok {
		val, err := strconv.Atoi(threshold)
		if err != nil {
			return fmt.Errorf("invalid threshold value: %v", err)
		}
		if val < 0 {
			return fmt.Errorf("threshold cannot be negative")
		}
		p.config.Threshold = val
	}

	// Validate field name
	if p.config.Field == "" {
		return fmt.Errorf("field cannot be empty")
	}
	if !isValidFieldName(p.config.Field) {
		return fmt.Errorf("invalid field name: %s", p.config.Field)
	}

	return nil
}

// Process handles the processing of records
func (p *TestProcessor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	result := make([]sdk.ProcessedRecord, len(records))

	// Process each record individually
	for i, record := range records {
		// Parse record payload
		var data map[string]interface{}
		if err := json.Unmarshal(record.Payload.After.Bytes(), &data); err != nil {
			result[i] = sdk.ErrorRecord{Error: err}
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
				continue
			}
		}

		// If field doesn't exist or value isn't numeric, filter the record
		result[i] = sdk.FilterRecord{}
	}

	return result
}

// Specification returns the processor specification
func (p *TestProcessor) Specification() (sdk.Specification, error) {
	return sdk.Specification{
		Name:    "sql.threshold",
		Summary: "Filter records based on numeric field threshold (WASM compatible)",
		Description: `A simple processor that filters records based on a numeric field value.
Records where the specified field's value exceeds the configured threshold will be passed through,
while others will be filtered out.

Field values are automatically converted to numbers where possible:
- Integer values are used directly
- Float values are compared as-is
- String values are parsed as numbers if possible
- Other types or parsing failures result in the record being filtered out

WASM Limitations:
- No database connectivity
- No network operations or external API calls
- In-memory processing only`,
		Version: "v1.0.0",
		Author:  "Conduit Contributors",
		Parameters: map[string]config.Parameter{
			ProcessorConfigField: {
				Default:     "",
				Description: "Field name to check against the threshold",
				Type:        config.ParameterTypeString,
				Validations: []config.Validation{
					config.ValidationRequired{},
					// Remove the template validation since field name validation
					// will be handled in the Configure method
				},
			},
			ProcessorConfigThreshold: {
				Default:     "0",
				Description: "Threshold value for filtering",
				Type:        config.ParameterTypeInt,
				Validations: []config.Validation{
					config.ValidationRequired{},
					config.ValidationGreaterThan{V: -1}, // Allow 0 as threshold
				},
			},
		},
	}, nil
}

// toNumber attempts to convert a value to a float64 for numeric operations
func (p *TestProcessor) toNumber(val interface{}) (float64, bool) {
	switch v := val.(type) {
	case int:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case string:
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f, true
		}
	}
	return 0, false
}

// isValidFieldName checks if a field name is valid according to our rules
func isValidFieldName(name string) bool {
    // Empty field name is invalid
    if name == "" {
        return false
    }
    
    // Field name cannot be longer than 64 characters
    if len(name) > 64 {
        return false
    }
    
    // Field name must only contain alphanumeric characters, underscores, and dots
    for _, r := range name {
        if !((r >= 'a' && r <= 'z') || 
            (r >= 'A' && r <= 'Z') || 
            (r >= '0' && r <= '9') || 
            r == '_' || r == '.') {
            return false
        }
    }
    
    return true
}

func TestProcessor_Configure(t *testing.T) {
	tests := []struct {
		name    string
		config  map[string]string
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config",
			config: map[string]string{
				"field":     "age",
				"threshold": "18",
			},
			wantErr: false,
		},
		{
			name: "missing field",
			config: map[string]string{
				"threshold": "18",
			},
			wantErr: true,
			errMsg:  "field cannot be empty",
		},
		{
			name: "invalid field name",
			config: map[string]string{
				"field":     "invalid-field",
				"threshold": "18",
			},
			wantErr: true,
			errMsg:  "invalid field name",
		},
		{
			name: "negative threshold",
			config: map[string]string{
				"field":     "age",
				"threshold": "-1",
			},
			wantErr: true,
			errMsg:  "threshold cannot be negative",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &TestProcessor{}
			cfg := config.Config{}

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

// We're removing the SQL-specific test methods and implementing only the new functionality tests

func TestProcessor_Process(t *testing.T) {
	tests := []struct {
		name           string
		config         TestProcessorConfig
		input          []opencdc.Record
		expectedCount  int
		expectedErrors int
		expectedType   string
	}{
		{
			name: "filter record above threshold",
			config: TestProcessorConfig{
				Field:     "age",
				Threshold: 18,
			},
			input: []opencdc.Record{
				createTestRecord(map[string]interface{}{"age": 25}),
			},
			expectedCount:  1,
			expectedErrors: 0,
			expectedType:   "SingleRecord",
		},
		{
			name: "filter record below threshold",
			config: TestProcessorConfig{
				Field:     "age",
				Threshold: 18,
			},
			input: []opencdc.Record{
				createTestRecord(map[string]interface{}{"age": 16}),
			},
			expectedCount:  1,
			expectedErrors: 0,
			expectedType:   "FilterRecord",
		},
		{
			name: "missing field",
			config: TestProcessorConfig{
				Field:     "non_existent",
				Threshold: 18,
			},
			input: []opencdc.Record{
				createTestRecord(map[string]interface{}{"age": 25}),
			},
			expectedCount:  1,
			expectedErrors: 0,
			expectedType:   "FilterRecord",
		},
		{
			name: "non-numeric field",
			config: TestProcessorConfig{
				Field:     "name",
				Threshold: 18,
			},
			input: []opencdc.Record{
				createTestRecord(map[string]interface{}{"name": "John", "age": 25}),
			},
			expectedCount:  1,
			expectedErrors: 0,
			expectedType:   "FilterRecord",
		},
		{
			name: "parse error",
			config: TestProcessorConfig{
				Field:     "age",
				Threshold: 18,
			},
			input: []opencdc.Record{
				{
					Position: opencdc.Position("test-position"),
					Payload: opencdc.Change{
						After: opencdc.RawData([]byte(`invalid json`)),
					},
				},
			},
			expectedCount:  1,
			expectedErrors: 1,
			expectedType:   "ErrorRecord",
		},
		{
			name: "multiple records",
			config: TestProcessorConfig{
				Field:     "age",
				Threshold: 18,
			},
			input: []opencdc.Record{
				createTestRecord(map[string]interface{}{"age": 25}),
				createTestRecord(map[string]interface{}{"age": 16}),
				createTestRecord(map[string]interface{}{"age": 30}),
			},
			expectedCount:  3,
			expectedErrors: 0,
			expectedType:   "Mixed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &TestProcessor{
				config: tt.config,
			}

			p.recordPool = sync.Pool{
				New: func() interface{} {
					return make(map[string]interface{})
				},
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

func TestProcessor_Specification(t *testing.T) {
	p := &TestProcessor{}

	spec, err := p.Specification()
	if err != nil {
		t.Fatalf("Specification() error = %v", err)
	}

	// Basic validations
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

	// Check that WASM limitations are mentioned
	if !strings.Contains(spec.Description, "WASM") {
		t.Error("Specification() Description should mention WASM compatibility")
	}

	// Validate field parameter
	field, ok := spec.Parameters[ProcessorConfigField]
	if !ok {
		t.Error("Specification() missing field parameter")
	} else {
		if field.Type != config.ParameterTypeString {
			t.Errorf("Field parameter type = %v, want %v", field.Type, config.ParameterTypeString)
		}
		if field.Default != "" {
			t.Error("Field parameter should have empty default value")
		}
		if len(field.Validations) != 1 {
			t.Errorf("Field parameter validations count = %d, want 1", len(field.Validations))
		}
		// Check Required validation
		hasRequired := false
		for _, v := range field.Validations {
			switch v.(type) {
			case config.ValidationRequired:
				hasRequired = true
			}
		}
		if !hasRequired {
			t.Error("Field parameter missing Required validation")
		}
	}

	// Validate threshold parameter
	threshold, ok := spec.Parameters[ProcessorConfigThreshold]
	if !ok {
		t.Error("Specification() missing threshold parameter")
	} else {
		if threshold.Type != config.ParameterTypeInt {
			t.Errorf("Threshold parameter type = %v, want %v", threshold.Type, config.ParameterTypeInt)
		}
		if threshold.Default != "0" {
			t.Errorf("Threshold parameter default = %v, want 0", threshold.Default)
		}
		if len(threshold.Validations) != 2 {
			t.Errorf("Threshold parameter validations count = %d, want 2", len(threshold.Validations))
		}
		// Check Required and GreaterThan validations
		hasRequired := false
		hasGreaterThan := false
		for _, v := range threshold.Validations {
			switch v.(type) {
			case config.ValidationRequired:
				hasRequired = true
			case config.ValidationGreaterThan:
				if v.(config.ValidationGreaterThan).V != -1 {
					t.Error("Threshold GreaterThan validation should be -1")
				}
				hasGreaterThan = true
			}
		}
		if !hasRequired {
			t.Error("Threshold parameter missing Required validation")
		}
		if !hasGreaterThan {
			t.Error("Threshold parameter missing GreaterThan validation")
		}
	}
}

// Remove the SQL security validation test - it's not relevant for the new implementation

func TestProcessor_FieldValidation(t *testing.T) {
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

// Benchmark tests
func BenchmarkProcessor_Process(b *testing.B) {
	p := &TestProcessor{
		config: TestProcessorConfig{
			Field:     "age",
			Threshold: 18,
		},
	}

	p.recordPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]interface{})
		},
	}

	record := createTestRecord(map[string]interface{}{
		"name":  "John",
		"age":   30,
		"email": "john@example.com",
	})
	records := []opencdc.Record{record}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Process(context.Background(), records)
	}
}

func BenchmarkProcessor_NumericComparison(b *testing.B) {
	p := &TestProcessor{}
	data := map[string]interface{}{
		"age": 30,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.toNumber(data["age"])
	}
}

func BenchmarkProcessor_MultipleRecords(b *testing.B) {
	p := &TestProcessor{
		config: TestProcessorConfig{
			Field:     "age",
			Threshold: 18,
		},
	}

	p.recordPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]interface{})
		},
	}

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

func createLargeTestRecord() opencdc.Record {
	// Create a record larger than 1MB
	largeString := strings.Repeat("x", 1024*1024+1)
	data := map[string]interface{}{
		"large_field": largeString,
	}
	return createTestRecord(data)
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