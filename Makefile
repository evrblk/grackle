.PHONY: build generate grackle clean grackle-image

DONT_FIND := -name .git -prune -o -name .cache -prune -o -name .pkg -prune -o

# Builds all artifacts
build:
	go vet ./...
	go fmt ./...
	go build ./...

# Generates protos and go:generate
generate:
	@echo "Running go generate..."
	go generate ./...
	@echo "Generating proto files..."
	protoc --proto_path=. \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-vtproto_out=. \
		--go-vtproto_opt=features=marshal+unmarshal+size \
		--go-vtproto_opt=paths=source_relative \
		./pkg/corepb/*.proto

grackle: build
	go build -o ./cmd/grackle/grackle ./cmd/grackle

format:
	find . $(DONT_FIND) -name '*.pb.go' \
		-type f -name '*.go' -exec gofmt -w -s {} \;
	find . $(DONT_FIND) -name '*.pb.go' \
		-type f -name '*.go' -exec goimports -w -local github.com/evrblk/grackle {} \;

clean:
	rm -rf cmd/grackle/grackle
	go clean ./...

grackle-image:
	docker build -t evrblk/grackle -f cmd/grackle/Dockerfile .
