# Overview

Everblack Grackle provides distributed synchronisation primitives:

* [Hierarchical locks](/docs/locks.md) - can be exclusively locked by a single process, or shared by multiple processes.
* [Weighted semaphores](/docs/semaphores.md) - tracks how many units of a particular resource are available.
* [Wait groups](/docs/wait-groups.md) - merge or fan-in of millions of tasks, similar to `sync.WaitGroup` in Go.
* [Barriers](/docs/barriers.md) - repeatedly wait for millions of processes to reach a certain point.

Grackle state is durable, and every primitive is reclaimed automatically: lock and semaphore holds
sit under a TTL lease that the holder heartbeats; wait groups have their own absolute deadline; and
barriers are auto-deleted after a configurable period of inactivity. A process crash will never
cause a dangling lock or a wait group that blocks waiters forever. All operations are atomic and
safe to retry.

Grackle is Open Source (under AGPL-3 license).

[Get started with Grackle](/docs/getting-started.md).
