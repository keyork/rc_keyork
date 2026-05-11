.PHONY: build run test test-verbose lint tidy

build:
	go build -o bin/server ./cmd/server

# Run with all mocks (no external infra needed), INFO level text logging
run:
	MOCK=true ROLE=all HTTP_ADDR=:8080 LOG_LEVEL=info LOG_FORMAT=text go run ./cmd/server

# Same but with DEBUG-level logging for tracing individual deliveries
run-debug:
	MOCK=true ROLE=all HTTP_ADDR=:8080 LOG_LEVEL=debug LOG_FORMAT=text go run ./cmd/server

# JSON-format logs (useful for piping to jq)
run-json:
	MOCK=true ROLE=all HTTP_ADDR=:8080 LOG_LEVEL=info LOG_FORMAT=json go run ./cmd/server

test:
	go test ./...

test-verbose:
	go test -v ./...

# Run a single package, e.g.: make test-pkg PKG=./internal/circuitbreaker
test-pkg:
	go test -v $(PKG)

lint:
	go vet ./...

tidy:
	go mod tidy
