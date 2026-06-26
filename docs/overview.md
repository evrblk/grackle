# Everblack Grackle

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
  
Grackle is Open Source, under the AGPL-3 license.

[Get started with Grackle →](/docs/getting-started.md)
