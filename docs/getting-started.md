# Getting Started

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

`node` processes don't take a cluster config file — they only need a data directory and a
gRPC listen address, and they start out **unprovisioned**. Cluster topology (which nodes exist,
which applications/shards/replicas they host) lives on the cluster itself and is pushed to nodes
over Monstera's admin plane. `gateway` and `worker` don't take a config file either: they take one
of `--monstera-nodes` / `--monstera-nodes-file` / `--monstera-nodes-srv` to discover a few live
nodes, then learn (and keep polling) the full cluster config from the cluster itself.

First, build a cluster config file locally — this is only a local artifact used to bootstrap the
cluster, not something any `grackle` process reads directly:

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

This creates `./cluster_config.json` with 3 nodes and 5 sharded application cores (each
replicated 3x by default — pass `--replication-factor` to `add-application` to change it). Take a
look inside to see how simple it is.

Next, start the (empty, unprovisioned) nodes — each just needs its own data directory and the
gRPC address it will listen on, matching what you put in the config:

```shell
$ ./grackle run node --data-dir=./data/node_01 --listen=localhost:7001
$ ./grackle run node --data-dir=./data/node_02 --listen=localhost:7002
$ ./grackle run node --data-dir=./data/node_03 --listen=localhost:7003
```

Then push the config to all of them in one step over the admin plane — this assigns each node its
`--node-id` from the config and is what flips them from `UNPROVISIONED` to `READY`:

```shell
$ go tool github.com/evrblk/monstera/cmd/monstera cluster bootstrap-nodes \
  --config=./cluster_config.json
```

Finally, start the stateless components. Instead of a config file, they take one or more live node
addresses to discover the cluster from (they then keep polling the cluster for topology changes,
so this doesn't need to be an exhaustive list):

```shell
$ ./grackle run worker --monstera-nodes=localhost:7001,localhost:7002,localhost:7003

$ ./grackle run gateway --port=8000 --monstera-nodes=localhost:7001,localhost:7002,localhost:7003
```

To add a node to a running cluster, move a shard's replica between nodes, or fetch the live
cluster config, use `monstera cluster add-node` / `move-shard` / `get-config` — see
[`tools/dev/compose-cluster/README.md`](/tools/dev/compose-cluster/README.md) for a worked example
of the full add-node-and-rebalance flow.

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
	LeaseId:        createLeaseResp.Lease.LeaseId,
	TimeoutSeconds: 60,
})
```

Conventions shared across all the APIs are described in the [API overview](/docs/api-overview.md).
