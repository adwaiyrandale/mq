# DMQ Makefile
# Provides targets for building, testing, and generating code

# Proto import paths
PROTOC_INCLUDES = -I. -I./third_party

.PHONY: all build proto gateway swagger clean test run-broker run-gateway

# Default target
all: build

# Build all binaries
build: build-broker build-gateway build-cli build-benchmark

build-broker:
	go build -o bin/broker ./cmd/broker

build-gateway:
	go build -o bin/gateway ./cmd/gateway

build-cli:
	go build -o bin/cli ./cmd/cli

build-benchmark:
	go build -o bin/benchmark ./cmd/benchmark

# Generate protobuf code
proto:
	@echo "Generating protobuf code..."
	@mkdir -p proto
	@protoc $(PROTOC_INCLUDES) \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		proto/messagequeue.proto

# Generate gRPC gateway code
gateway: proto
	@echo "Generating gRPC gateway code..."
	@protoc $(PROTOC_INCLUDES) \
		--grpc-gateway_out=. \
		--grpc-gateway_opt=paths=source_relative \
		--grpc-gateway_opt=generate_unbound_methods=true \
		proto/messagequeue.proto

# Generate OpenAPI/Swagger spec
swagger: proto
	@echo "Generating OpenAPI spec..."
	@mkdir -p api/swagger
	@protoc $(PROTOC_INCLUDES) \
		--openapiv2_out=api/swagger \
		--openapiv2_opt=allow_merge=true \
		--openapiv2_opt=merge_file_name=swagger \
		--openapiv2_opt=use_go_templates=true \
		proto/messagequeue.proto
	@mv api/swagger/swagger.swagger.json api/swagger/swagger.json 2>/dev/null || true
	@echo "Swagger spec generated at api/swagger/swagger.json"

# Generate all code from protobuf
generate: proto gateway swagger

# Run tests
test:
	go test ./...

# Clean generated files
clean:
	rm -rf bin/
	rm -f proto/*.pb.go
	rm -f api/swagger/*.json

# Run single broker
run-broker:
	go run ./cmd/broker -id=broker-1 -port=9001 -data=./data

# Run gateway (requires broker running)
run-gateway:
	go run ./cmd/gateway

# Run both (in background)
run: build
	./bin/broker -id=broker-1 -port=9001 -data=./data &
	@sleep 2
	./bin/gateway

# Install dependencies
deps:
	go mod tidy
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest

# Quick test with curl
quick-test:
	@echo "Creating topic..."
	@curl -s -X POST http://localhost:8080/v1/topics \
		-H "Content-Type: application/json" \
		-d '{"topic":"test-topic","partitions":3}' | jq .
	@echo "\nListing topics..."
	@curl -s http://localhost:8080/v1/topics | jq .
	@echo "\nProducing message..."
	@curl -s -X POST http://localhost:8080/v1/topics/test-topic/messages \
		-H "Content-Type: application/json" \
		-d '{"key":"a2V5","value":"aGVsbG8gd29ybGQ=","acks":1}' | jq .
	@echo "\nConsuming messages..."
	@curl -s "http://localhost:8080/v1/topics/test-topic/partitions/0/messages?offset=0" | jq .
