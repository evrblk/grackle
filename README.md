# Everblack Grackle

[![Go](https://github.com/evrblk/grackle/actions/workflows/go.yml/badge.svg)](https://github.com/evrblk/grackle/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/evrblk/grackle)](https://goreportcard.com/report/github.com/evrblk/grackle)

Everblack Grackle is a distributed-synchronization-primitives-as-a-service:

* __read/write locks__ (can be exclusively locked for writing by a single process, or it can be locked for reading by multiple processes)
* __semaphores__ (tracks how many units of a particular resource are available)
* __wait groups__ (merge or fan-in of millions of tasks, similar to sync.WaitGroup in Go)

Grackle state is durable. All holds have a set expiration time. Process crash will not cause a dangling lock. 
Long-running processes can extend the hold. All operations are atomic and safe to retry.

Grackle can operate in a clustered mode (with replication and sharding), or it can run in a single-process nonclustered 
mode (full state on disk, no replication, no sharding). It has no external dependencies (no databases, no kafka, no redis, no zookeeper, 
or whatever) and it stores all its state on disk.

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

### Nonclustered mode

```shell
$ ./grackle run nonclustered --port=8000 --data-dir=./data
```

### Clustered mode

There are 3 components: 

* `gateway` stateless API gateway
* `node` stateful node with data persisted on the disk
* `worker` stateless async worker

Running in the clustered mode requires Monstera cluster config file. To generate a simple config run:

```shell
$ go tool github.com/evrblk/monstera/cmd/monstera config init \
  --node=localhost:7001 \
  --node=localhost:7002 \
  --node=localhost:7003 \
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
  --name=GrackleNamespaces \
  --implementation=GrackleNamespaces \
  --shards-count=8
```

This will create `./cluster_config.json` file with 3 nodes and 4 sharded application cores that are parts of Grackle. 
Take a look inside to see how actually simple it is.

Then run all components:

```shell
$ ./grackle run node --node-address=localhost:7001 --data-dir=./data/node1 --monstera-config=./cluster_config.json
$ ./grackle run node --node-address=localhost:7002 --data-dir=./data/node2 --monstera-config=./cluster_config.json
$ ./grackle run node --node-address=localhost:7003 --data-dir=./data/node3 --monstera-config=./cluster_config.json

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
	"time"
    evrblk "github.com/evrblk/evrblk-go"
    grackle "github.com/evrblk/evrblk-go/grackle/preview"
)

grackleClient := grackle.NewGrackleGrpcClient("localhost:8000", evrblk.NewNoOpSigner())

acquireLockResp, err := grackleClient.AcquireLock(context.Background(), &grackle.AcquireLockRequest{
    NamespaceName: "my_namespace"
	LockName: "lock1",
	WriteLock: true,
    ProcessId: "process1",
	ExpiresAt: time.Now().Add(5 * time.Minute).UnixNano()
})
```

## Authentication

By default, API calls are unauthenticated. To use request signing add `--auth-keys-path=` argument to 
`./grackle run gateway` or `./grackle run nonclustered`. It should point to a directory with API keys where each file 
name is an API key ID, and corresponding file content is an API secret key.

Generate keys with `evrblk` [CLI tool](https://github.com/evrblk/evrblk-cli):

```shell
$ evrblk authn generate-alfa-key
```

Read [Authentication](https://everblack.dev/docs/api/authentication/) documentation to learm more about how it works 
and how to generate keys.

## License

Everblack Grackle is released under the [AGPL-3 License](https://opensource.org/license/agpl-v3).
