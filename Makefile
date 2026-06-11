.PHONY: build generate grackle clean grackle-image

DONT_FIND := -name .git -prune -o -name .cache -prune -o -name .pkg -prune -o

# Lint, static checks, vuln shecks
lint:
	go fmt ./...
	go vet ./...
	staticcheck ./...
	govulncheck ./...

# Builds all artifacts
build:
	go build ./...

# Generates protos and go:generate
generate:
	@echo "Generating proto files..."
	protoc --proto_path=. \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-vtproto_out=. \
		--go-vtproto_opt=features=marshal+unmarshal+size \
		--go-vtproto_opt=paths=source_relative \
		./pkg/corepb/*.proto

	@echo "Generating Monstera stubs and adapters implementations..."
	cd ./pkg/coreapis; go tool github.com/evrblk/monstera/cmd/monstera code generate

	@echo "Generating Marshal/Unmarshal implementations..."
	go run ./tools/codegen/genmarshal -dir ./pkg/corepb -output ./pkg/corepb/marshal_gen.go

grackle: build
	go build -o ./cmd/grackle/grackle ./cmd/grackle

format:
	find . $(DONT_FIND) -name '*.pb.go' \
		-type f -name '*.go' -exec gofmt -w -s {} \;
	find . $(DONT_FIND) -name '*.pb.go' \
		-type f -name '*.go' -exec goimports -w -local github.com/evrblk/grackle {} \;

clean:
	rm -rf cmd/grackle/grackle
	rm -rf ./.data
	rm -rf ./tools/dev/debug-cluster/.data
	rm -rf ./tools/dev/compose-cluster/.data
	rm -rf ./tools/dev/compose-cluster/grackle
	rm -rf ./tools/dev/load-generator/load-generator
	go clean ./...

grackle-image:
	docker build -t evrblk/grackle -f cmd/grackle/Dockerfile .
