---
title: Overview
type: docs
layout: grackle
aliases:
  - /docs/grackle/
---

Everblack Grackle provides distributed synchronisation primitives:

* [Hierarchical locks](/docs/locks) - can be exclusively locked by a single process, or shared by multiple processes.
* [Weighted semaphores](/docs/semaphores) - tracks how many units of a particular resource are available.
* [Wait groups](/docs/wait-groups) - merge or fan-in of millions of tasks, similar to `sync.WaitGroup` in Go.
* [Barriers](/docs/barriers) - repeatedly wait for millions of processes to reach a certain point.

Grackle state is durable, and every primitive has a built-in expiration: lock and semaphore holds
sit under a TTL lease that the holder heartbeats; wait groups and barriers have their own absolute
deadline. A process crash will never cause a dangling lock or a wait group that blocks waiters
forever. All operations are atomic and safe to retry.

Grackle is Open Source (under AGPL-3 license).

[Get started with Grackle](/docs/getting-started).
