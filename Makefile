.PHONY: build generate-proto

# Build task: runs proto generation, go generate, and then builds all artifacts
build: generate-proto
	@echo "Running go generate..."
	go generate ./...
	go vet ./...
	go fmt ./...
	go build ./...

generate-proto:
	@echo "Generating proto files..."
	$(eval MONSTERA_PROTO_ROOT := $(shell go list -f '{{.Dir}}' -m github.com/evrblk/monstera))
	protoc --proto_path=. --proto_path="$(MONSTERA_PROTO_ROOT)" --go_out=. --go_opt=paths=source_relative ./pkg/corepb/*.proto
