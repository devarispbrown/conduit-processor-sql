package sql

//go:generate go run github.com/conduitio/conduit-commons/paramgen -output=paramgen_proc.go ProcessorConfig

// ProcessorConfig holds the configuration for the SQL processor
type ProcessorConfig struct {
	// Field is the target field that will be set
	Field string `json:"field"`
	// Threshold is the threshold for filtering the record
	Threshold int `json:"threshold"`
}

