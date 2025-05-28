VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build test lint clean install-tools

# Build the processor as a WASM binary
build:
	@echo "Building SQL processor..."
	GOOS=wasip1 GOARCH=wasm go build -tags wasm -o conduit-processor-sql.wasm ./main.go

# Run tests
test:
	go test -v ./sql/...

# Run tests with coverage
test-coverage:
	go test -v -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Lint the code
lint:
	golangci-lint run

# Clean build artifacts
clean:
	rm -f conduit-processor-sql.wasm coverage.out coverage.html

# Install development tools
install-tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

# Format code
fmt:
	go fmt ./...

# Check for security issues
security:
	gosec ./...

# Run all checks (format, lint, test)
check: fmt lint test

# Build for development (regular binary for testing)
build-dev:
	go build -o conduit-processor-sql .

# Run processor with example config (for development)
run-example: build-dev
	./conduit-processor-sql --config example-config.yaml

# Generate mocks for testing
generate-mocks:
	go generate ./...

# Update dependencies
update-deps:
	go get -u ./...
	go mod tidy

# Verify module dependencies
verify:
	go mod verify

# Show help
help:
	@echo "Available targets:"
	@echo "  build         - Build WASM binary for Conduit"
	@echo "  build-dev     - Build regular binary for development"
	@echo "  test          - Run tests"
	@echo "  test-coverage - Run tests with coverage report"
	@echo "  lint          - Run linter"
	@echo "  fmt           - Format code"
	@echo "  clean         - Clean build artifacts"
	@echo "  install-tools - Install development tools"
	@echo "  check         - Run format, lint, and test"
	@echo "  security      - Check for security issues"
	@echo "  update-deps   - Update dependencies"
	@echo "  verify        - Verify module dependencies"
	@echo "  help          - Show this help"

# Default target
all: check build