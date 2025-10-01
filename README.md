# Everblack Grackle

[![Go](https://github.com/evrblk/grackle/actions/workflows/go.yml/badge.svg)](https://github.com/evrblk/grackle/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/evrblk/grackle)](https://goreportcard.com/report/github.com/evrblk/grackle)

Everblack Grackle provides distributed synchronisation primitives:

* read/write locks
* semaphores
* wait groups

All holds have a set expiration time. Process crash will not cause a dangling lock. Long-running processes can extend 
the hold. All operations are atomic and safe to retry.

Go to [official documentation](https://everblack.dev/docs/grackle) to learn more.

## Running

Grackle can operate in a cluster mode (with replication and sharding):

* `grackle run gateway`
* `grackle run node`
* `grackle run worker`

Alternatively it can run in a single-binary non-clustered mode (full state on disk, no replication, no sharding):

* `grackle run single`

## Building

Grackle is powered by [Monstera](https://github.com/evrblk/monstera) framework. Building steps include Protobuf 
generation, Monstera code generation, and Go build. To run all of that at once:

```
make build
```

## License

Everblack Grackle is released under the [AGPL-3 License](https://opensource.org/license/agpl-v3).
