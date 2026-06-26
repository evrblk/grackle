# Everblack Grackle

[![Go](https://github.com/evrblk/grackle/actions/workflows/go.yml/badge.svg)](https://github.com/evrblk/grackle/actions/workflows/go.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/evrblk/grackle)](https://goreportcard.com/report/github.com/evrblk/grackle)

**Everblack Grackle is the coordination layer for your distributed system** — fundamental
synchronization primitives, served over a clean API, in a single self-contained binary.

Stop reinventing distributed locks on top of a database, or bending a key-value store into a
semaphore. Grackle gives you the primitives directly, with the durability and safety guarantees you'd
otherwise have to build (and debug) yourself.

## The primitives

* **[Hierarchical locks](/docs/locks.md)** — shared (read) or exclusive (write) locks whose names are
  `/`-separated paths. Lock `users/123` and you've guarded its whole subtree — no need to enumerate
  every leaf. When an acquire is blocked, Grackle tells you *who* holds the lock and *why*.
* **[Weighted semaphores](/docs/semaphores.md)** — bound concurrent access to a resource pool. Each
  acquire takes a configurable *weight*, so heavy and light work can share one pool of permits.
* **[Wait groups](/docs/wait-groups.md)** — fan-in for millions of jobs, the distributed
  equivalent of Go's `sync.WaitGroup`: workers report jobs done, observers block until the group
  completes.
* **[Barriers](/docs/barriers.md)** — generational, reusable rendezvous points where a fleet of
  processes wait for each other and advance together, cycle after cycle.

Everything lives inside a **namespace** and is reached by name — no IDs to track, no resources to
provision up front.

## Why Grackle

* **Safe by default.** Lock and semaphore holds sit under a TTL **lease** that the holder heartbeats;
  wait groups carry an absolute deadline; idle barriers auto-delete. A crashed client never leaves a
  dangling lock or a wait group that blocks forever. Every operation is atomic and safe to retry.
* **Durable and replicated.** State is persisted and Raft-replicated for high availability, and
  sharded so it scales horizontally as your workload grows.
* **Zero dependencies.** One binary, embedded storage — no database, Kafka, Redis, or ZooKeeper to
  run alongside it. Start single-node, grow into a replicated, sharded cluster without changing your
  code.
* **An API that does the heavy lifting.** Responses are rich enough to act on — a failed lock acquire 
  returns the current holders and the reason it was blocked, not just "no". Consistent conventions 
  across every primitive: opaque [metadata](/docs/api-overview.md#metadata) on everything, 
  [optimistic-concurrency updates](/docs/api-overview.md#updates), cursor 
  [pagination](/docs/api-overview.md#pagination).

Go to [documentation](/docs/overview.md) to learn more.

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

### Clustered mode

Refer to [Getting Started](/docs/getting-started.md#clustered-mode) doc.

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

## Project Status

Grackle is being actively developed. However, feature-wise it already has the shape I originally envisioned for it.
Public API version is `v1beta` - expect some breaking changes before it becomes `v1`. Disk-level compatibility can
also be broken before `v1`.

## Contributing

Ways to contribute:

- Bug reports: Use the [GitHub issue tracker](https://github.com/evrblk/grackle/issues/new).
- Feature requests: Use the [GitHub issue tracker](https://github.com/evrblk/grackle/issues/new).
- Code changes: Submit a pull request.
- Documentation: Submit a pull request.

Before you contribute:

- Check [open issues](https://github.com/evrblk/grackle/issues) and 
  [pull requests](https://github.com/evrblk/grackle/pulls) to avoid duplicating work.

## License

Everblack Grackle is released under the [AGPL-3 License](https://opensource.org/license/agpl-v3).
