# Everblack Grackle

[![Go](https://github.com/evrblk/grackle/actions/workflows/go.yml/badge.svg)](https://github.com/evrblk/grackle/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/evrblk/grackle)](https://goreportcard.com/report/github.com/evrblk/grackle)

Everblack Grackle provides distributed synchronisation primitives:

* read/write locks
* semaphores
* wait groups

Grackle state is durable (on disk). All holds have a set expiration time. Process crash will not cause a dangling lock. 
Long-running processes can extend the hold. All operations are atomic and safe to retry.

Go to [official documentation](https://everblack.dev/docs/grackle) to learn more.

## Running

Build Grackle from sources:

```shell
make build
```


```shell
go run ./cmd/grackle
```

Grackle can operate in a cluster mode (with replication and sharding):

* `grackle run gateway`
* `grackle run node`
* `grackle run worker`

Alternatively it can run in a single-node non-clustered mode (full state on disk, no replication, no sharding):

* `grackle run single`

Running in cluster mode requires Monstera cluster config file. To generate a simple config for 3 nodes run:

```shell
go run ./cmd/grackle make-monstera-cluster \
  --node=localhost:7001 \
  --node=localhost:7002 \
  --node=localhost:7003 \
  --output=./cluster_config.json
```

## Using

Use official Everblack SDKs:

* https://github.com/evrblk/evrblk-go for Go
* https://github.com/evrblk/evrblk-ruby for Ruby

## License

Everblack Grackle is released under the [AGPL-3 License](https://opensource.org/license/agpl-v3).
