VERSION=$(shell git describe --tags --dirty --always)
BINARY_NAME=conduit-processor-sql
WASM_BINARY=$(BINARY_NAME).wasm

# Go build settings
GO_BUILD_ENV=CGO_ENABLED=0
WASM_ENV=GOOS=wasip1 GOARCH=wasm
GOTEST_FLAGS?=-v -count=1

# Directories
BUILD_DIR=./build
COVERAGE_DIR=./coverage

# Colors for output
GREEN=\033[0;32m
YELLOW=\033[1;33m
RED=\033[0;31m
NC=\033[0m # No Color

.PHONY: build test lint clean install-tools help
.PHONY: dev check coverage bench profile

# Default target
all: check build

## Essential Commands

# Build WASM binary for Conduit (primary build)
build:
	@echo "$(GREEN)Building WASM binary for Conduit...$(NC)"
	$(WASM_ENV) $(GO_BUILD_ENV) go build -tags wasm -ldflags "-s -w -X 'main.version=$(VERSION)'" -o $(WASM_BINARY) ./cmd/processor/main.go
	@echo "$(GREEN)✓ WASM binary built: $(WASM_BINARY)$(NC)"

# Run all tests
test:
	@echo "$(GREEN)Running tests...$(NC)"
	go test $(GOTEST_FLAGS) ./...

# Run all quality checks
check: fmt lint test
	@echo "$(GREEN)✓ All quality checks passed$(NC)"

# Clean build artifacts
clean:
	@echo "$(GREEN)Cleaning build artifacts...$(NC)"
	rm -f $(WASM_BINARY)
	rm -rf $(BUILD_DIR) $(COVERAGE_DIR)
	rm -f coverage.out coverage.html cpu.prof mem.prof
	go clean -cache -testcache
	@echo "$(GREEN)✓ Cleaned$(NC)"

## Development Commands

# Quick development cycle
dev: fmt test build
	@echo "$(GREEN)✓ Development cycle complete$(NC)"

# Format code
fmt:
	@echo "$(GREEN)Formatting code...$(NC)"
	go fmt ./...

# Run linter
lint:
	@echo "$(GREEN)Running linter...$(NC)"
	golangci-lint run --config .golangci.yml

# Generate code
generate:
	@echo "$(GREEN)Generating code...$(NC)"
	go generate ./...

# Install development tools
install-tools:
	@echo "$(GREEN)Installing development tools...$(NC)"
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install mvdan.cc/gofumpt@latest
	go install golang.org/x/tools/cmd/goimports@latest
	@echo "$(GREEN)✓ Development tools installed$(NC)"

## Testing Commands

# Run tests with coverage
coverage:
	@echo "$(GREEN)Running tests with coverage...$(NC)"
	@mkdir -p $(COVERAGE_DIR)
	go test -v -coverprofile=$(COVERAGE_DIR)/coverage.out -covermode=atomic ./...
	go tool cover -html=$(COVERAGE_DIR)/coverage.out -o $(COVERAGE_DIR)/coverage.html
	@echo "$(GREEN)✓ Coverage report: $(COVERAGE_DIR)/coverage.html$(NC)"

# Run SQL operation tests
test-sql:
	@echo "$(GREEN)Running SQL operation tests...$(NC)"
	go test $(GOTEST_FLAGS) -run "TestProcessor_(Transform|Filter|EvaluateExpression|ParseFieldList)" .

# Run expression evaluation tests
test-expressions:
	@echo "$(GREEN)Running expression tests...$(NC)"
	go test $(GOTEST_FLAGS) -run TestProcessor_EvaluateExpression .

# Run transform tests
test-transforms:
	@echo "$(GREEN)Running transform tests...$(NC)"
	go test $(GOTEST_FLAGS) -run TestProcessor_TransformRecord .

# Run filter tests  
test-filters:
	@echo "$(GREEN)Running filter tests...$(NC)"
	go test $(GOTEST_FLAGS) -run TestProcessor_FilterRecord .

## Performance Commands

# Run benchmarks
bench:
	@echo "$(GREEN)Running benchmarks...$(NC)"
	go test -bench=. -benchmem -benchtime=5s ./...

# Profile memory usage
profile-mem:
	@echo "$(GREEN)Profiling memory usage...$(NC)"
	go test -memprofile=mem.prof -bench=BenchmarkProcessor_Process .
	@echo "$(GREEN)Memory profile saved to mem.prof$(NC)"
	@echo "$(YELLOW)View with: go tool pprof mem.prof$(NC)"

# Profile CPU usage
profile-cpu:
	@echo "$(GREEN)Profiling CPU usage...$(NC)"
	go test -cpuprofile=cpu.prof -bench=BenchmarkProcessor_Process .
	@echo "$(GREEN)CPU profile saved to cpu.prof$(NC)"
	@echo "$(YELLOW)View with: go tool pprof cpu.prof$(NC)"

## Utility Commands

# Build development binary
build-dev:
	@echo "$(GREEN)Building development binary...$(NC)"
	@mkdir -p $(BUILD_DIR)
	$(GO_BUILD_ENV) go build -ldflags "-X 'main.version=$(VERSION)'" -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/processor/main.go
	@echo "$(GREEN)✓ Development binary: $(BUILD_DIR)/$(BINARY_NAME)$(NC)"

# Update dependencies
update-deps:
	@echo "$(GREEN)Updating dependencies...$(NC)"
	go get -u ./...
	go mod tidy

# Verify dependencies
verify:
	@echo "$(GREEN)Verifying dependencies...$(NC)"
	go mod verify

# Security scan
security:
	@echo "$(GREEN)Running security scan...$(NC)"
	@command -v gosec >/dev/null 2>&1 || { echo "$(YELLOW)Installing gosec...$(NC)"; go install github.com/securecodewarrior/gosec/v2/cmd/gosec@latest; }
	gosec ./...

# Show project info
info:
	@echo "$(GREEN)Project Information:$(NC)"
	@echo "Version: $(VERSION)"
	@echo "Binary: $(WASM_BINARY)"
	@echo "Go Version: $(shell go version)"
	@echo "Git Commit: $(shell git rev-parse --short HEAD 2>/dev/null || echo 'unknown')"

# Pre-commit checks
pre-commit: fmt lint test
	@echo "$(GREEN)✓ Pre-commit checks passed$(NC)"

# Watch for changes (requires entr)
watch:
	@echo "$(GREEN)Watching for changes... (Press Ctrl+C to stop)$(NC)"
	@command -v entr >/dev/null 2>&1 || { echo "$(RED)Error: entr not found. Install with: brew install entr$(NC)"; exit 1; }
	find . -name "*.go" | entr -c make test

# Show help
help:
	@echo "$(GREEN)Conduit SQL Processor (WASM) - Available Commands:$(NC)"
	@echo ""
	@echo "$(YELLOW)Essential Commands:$(NC)"
	@echo "  build          - Build WASM binary for Conduit"
	@echo "  test           - Run all tests"
	@echo "  check          - Run format, lint, and test"
	@echo "  clean          - Clean build artifacts"
	@echo ""
	@echo "$(YELLOW)Development:$(NC)"
	@echo "  dev            - Quick development cycle (fmt + test + build)"
	@echo "  fmt            - Format code"
	@echo "  lint           - Run linter"
	@echo "  generate       - Generate code"
	@echo "  install-tools  - Install development tools"
	@echo ""
	@echo "$(YELLOW)Testing:$(NC)"
	@echo "  coverage       - Run tests with coverage report"
	@echo "  test-sql       - Run SQL operation tests"
	@echo "  test-expressions - Run expression evaluation tests"
	@echo "  test-transforms - Run transform tests"
	@echo "  test-filters   - Run filter tests"
	@echo ""
	@echo "$(YELLOW)Performance:$(NC)"
	@echo "  bench          - Run benchmarks"
	@echo "  profile-mem    - Profile memory usage"
	@echo "  profile-cpu    - Profile CPU usage"
	@echo ""
	@echo "$(YELLOW)Utilities:$(NC)"
	@echo "  build-dev      - Build development binary"
	@echo "  update-deps    - Update dependencies"
	@echo "  verify         - Verify dependencies"
	@echo "  security       - Run security scan"
	@echo "  info           - Show project information"
	@echo "  pre-commit     - Run pre-commit checks"
	@echo "  watch          - Watch for changes and auto-test"
	@echo ""
	@echo "$(YELLOW)Examples:$(NC)"
	@echo "  make build                    # Build WASM binary"
	@echo "  make test GOTEST_FLAGS=\"-v\"   # Run tests with verbose output"
	@echo "  make coverage                 # Generate coverage report"
	@echo "  make dev                      # Quick development cycle"
	@echo "  make watch                    # Auto-test on file changes"