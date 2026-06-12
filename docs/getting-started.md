---
title: Getting Started
type: docs
layout: grackle
---

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

Grackle can operate in a cluster mode (with replication and sharding), or it can run in a single-node nonclustered 
mode (full state on disk, no replication, no sharding). It has no external dependencies (no databases, no kafka, no redis, 
no zookeeper, or whatever) and it stores all its state on disk (on embedded BadgerDB).

### Single-node mode

```shell
$ ./grackle run single-node --port=8000 --data-dir=./data
```

### Clustered mode

There are 3 components:

* `gateway` stateless API gateway
* `node` stateful node with data persisted on disk
* `worker` stateless async worker

Running in the clustered mode requires Monstera cluster config file. To generate a simple config run:

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

This will create `./cluster_config.json` file with 3 nodes and 5 sharded application cores that are parts of Grackle.
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
$ evrblk grackle-v1beta list-namespaces --endpoint=localhost:8000
{}

$ echo '{"name": "name1"}' | evrblk grackle-v1beta create-namespace --endpoint=localhost:8000
{
  "namespace":  {
    "name":  "name1",
    "createdAt":  "1760464456161083000",
    "updatedAt":  "1760464456161083000"
  }
}

$ echo '{"namespace_name": "name1"}' | evrblk grackle-v1beta list-locks --endpoint=localhost:8000
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
    grackle "github.com/evrblk/evrblk-go/grackle/v1beta"
)

grackleClient := grackle.NewGrackleGrpcClient("localhost:8000", evrblk.NewNoOpSigner())

createLeaseResp, err := grackleClient.CreateLockLease(context.Background(), &grackle.CreateLockLeaseRequest{
	NamespaceName: "my_namespace",
	ProcessId:     "process1",
	TtlSeconds:     30,
})

acquireLockResp, err := grackleClient.AcquireLock(context.Background(), &grackle.AcquireLockRequest{
	NamespaceName:  "my_namespace",
	LockName:       "lock1",
	Exclusive:      true,
	LeaseId:        createLeaseResp.Lease.Id,
	TimeoutSeconds: 60,
})
```
