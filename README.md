# Everblack Grackle

[![Go](https://github.com/evrblk/grackle/actions/workflows/go.yml/badge.svg)](https://github.com/evrblk/grackle/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/evrblk/grackle)](https://goreportcard.com/report/github.com/evrblk/grackle)

Everblack Grackle is a distributed-synchronization-primitives-as-a-service:

* __hierarchical locks__ (can be exclusively locked by a single process, or shared by multiple processes)
* __weighted semaphores__ (tracks how many units of a particular resource are available)
* __wait groups__ (merge or fan-in of millions of tasks, similar to `sync.WaitGroup` in Go)
* __barriers__ (wait for millions of processes to reach a certain point)

Grackle state is durable. All holds are lease-based, with a set expiration time. Process crash will not cause dangling locks. 
Long-running processes can extend their leases. All operations are atomic and safe to retry.

Grackle can operate in a cluster mode (with replication and sharding), or it can run in a single-node nonclustered 
mode (full state on disk, no replication, no sharding). It has no external dependencies (no databases, no kafka, no redis, 
no zookeeper, or whatever) and it stores all its state on disk (on embedded BadgerDB).

Go to [official documentation](https://everblack.dev/docs/grackle) to learn more.

## Installing

Build Grackle from sources:

```shell
# Checkout source code
$ git clone git@github.com:evrblk/grackle.git
$ cd grackle

# Build
$ make build

# Produce ./grackle executable
$ make grackle
```

## Running

### Single-node mode

```shell
$ ./grackle run single-node --port=8000 --data-dir=./data
```

### Cluster mode

There are 3 components: 

* `gateway` stateless API gateway
* `node` stateful node with data persisted on disk
* `worker` stateless async worker

Running in the cluster mode requires Monstera cluster config file. To generate a simple config run:

```shell
$ go tool github.com/evrblk/monstera/cmd/monstera config init \
  --node-id=node_01 --node-address=localhost:7001 \
  --node-id=node_02 --node-address=localhost:7002 \
  --node-id=node_03 --node-address=localhost:7003 \
  --output=./cluster_config.json

$ go tool github.com/evrblk/monstera/cmd/monstera config add-application \
  --config=./cluster_config.json \
  --name=GrackleLocks \
  --implementation=GrackleLocks \
  --shards-count=16
  
$ go tool github.com/evrblk/monstera/cmd/monstera config add-application \
  --config=./cluster_config.json \
  --name=GrackleSemaphores \
  --implementation=GrackleSemaphores \
  --shards-count=16
  
$ go tool github.com/evrblk/monstera/cmd/monstera config add-application \
  --config=./cluster_config.json \
  --name=GrackleWaitGroups \
  --implementation=GrackleWaitGroups \
  --shards-count=16

$ go tool github.com/evrblk/monstera/cmd/monstera config add-application \
  --config=./cluster_config.json \
  --name=GrackleBarriers \
  --implementation=GrackleBarriers \
  --shards-count=16
  
$ go tool github.com/evrblk/monstera/cmd/monstera config add-application \
  --config=./cluster_config.json \
  --name=GrackleNamespaces \
  --implementation=GrackleNamespaces \
  --shards-count=8
```

This will create `./cluster_config.json` file with 3 nodes and 4 sharded application cores that are parts of Grackle. 
Take a look inside to see how actually simple it is.

Then run all components:

```shell
$ ./grackle run node --node-id=node_01 --data-dir=./data/node_01 --monstera-config=./cluster_config.json
$ ./grackle run node --node-id=node_02 --data-dir=./data/node_02 --monstera-config=./cluster_config.json
$ ./grackle run node --node-id=node_03 --data-dir=./data/node_03 --monstera-config=./cluster_config.json

$ ./grackle run worker --monstera-config=./cluster_config.json

$ ./grackle run gateway --port=8000 --monstera-config=./cluster_config.json
```

## Using

Use with `evrblk` CLI tool from [github.com/evrblk/evrblk-cli](https://github.com/evrblk/evrblk-cli).

Example:

```shell
$ evrblk grackle-preview list-namespaces --endpoint=localhost:8000
{}

$ echo '{"name": "name1"}' | evrblk grackle-preview create-namespace --endpoint=localhost:8000
{
  "namespace":  {
    "name":  "name1",
    "createdAt":  "1760464456161083000",
    "updatedAt":  "1760464456161083000"
  }
}

$ echo '{"namespace_name": "name1"}' | evrblk grackle-preview list-locks --endpoint=localhost:8000
{}
```

Or use with official Everblack SDKs:
* [github.com/evrblk/evrblk-go](https://github.com/evrblk/evrblk-go) for Go
* [github.com/evrblk/evrblk-ruby](https://github.com/evrblk/evrblk-ruby) for Ruby

Example in Go:

```go
import (
    evrblk "github.com/evrblk/evrblk-go"
    grackle "github.com/evrblk/evrblk-go/grackle/preview"
)

grackleClient := grackle.NewGrackleGrpcClient("localhost:8000", evrblk.NewNoOpSigner())

createLeaseResp, err := grackleClient.CreateLockLease(context.Background(), &grackle.CreateLockLeaseRequest{
	NamespaceName: "my_namespace",
	ProcessId:     "process1",
	TtlSeconds:     30,
})

acquireLockResp, err := grackleClient.AcquireLock(context.Background(), &grackle.AcquireLockRequest{
	NamespaceName: "my_namespace",
	LockName:      "lock1",
	WriteLock:     true,
	LeaseId:       createLeaseResp.Lease.Id,
})
```

## Authentication

By default, API calls are unauthenticated. To use request signing add `--auth-keys-path=` argument to 
`./grackle run gateway` or `./grackle run single-node`. It should point to a directory with API keys where each file 
name is an API key ID, and corresponding file content is an API secret key.

Generate keys with `evrblk` [CLI tool](https://github.com/evrblk/evrblk-cli):

```shell
$ evrblk authn generate-alfa-key
```

Read [Authentication](https://everblack.dev/docs/api/authentication/) documentation to learm more about how it works 
and how to generate keys.

## License

Everblack Grackle is released under the [AGPL-3 License](https://opensource.org/license/agpl-v3).
