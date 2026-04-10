# Grackle Development Guide

## Build & Test Commands

```bash
make grackle                  # fully build Grackle only
make generate-proto           # generate all protobufs
go test ./...                 # run all tests with Go directly
make format                   # format code (gofmt and goimports)
```

## Code Style Guidelines
- Follow standard Go formatting (gofmt/goimports)
- Import order: standard lib, external packages, then `evrblk/grackle` packages
- Error handling: Always check errors with `if err != nil { return ... }`
- Document all exported functions, types, and variables
- Use table-driven tests when appropriate
