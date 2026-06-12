---
title: ArriveAtBarrier
type: docs
layout: grackle
---

# ArriveAtBarrier

Records a peer's arrival at the barrier for a specific generation. Returns **immediately**
(non-blocking) with the current state and `all_arrived` indicating whether the barrier has
released for this generation. If the peer needs to block until the rendezvous completes, 
follow this with a `WaitAtBarrier` call.

The call is idempotent per `(generation, process_id)` — a retry by the same peer in the same
generation is a no-op.

## Request

* `process_id` is a free-form string identifying the peer.
* `expected_generation` must match the barrier's current generation; arriving for a past or
  future generation is rejected.

```json
{
  "namespace_name": "pipelines",
  "barrier_name": "phase_1_complete",
  "process_id": "shard-0",
  "expected_generation": 1
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the barrier does not exist.

__Not yet released:__

```json
{
  "barrier": {
    "name": "phase_1_complete",
    "expected_processes": 4,
    "arrived_processes": 1,
    "generation": 1
  },
  "all_arrived": false,
  "next_generation": 2
}
```

__This caller's arrival completed the rendezvous:__

```json
{
  "barrier": {
    "name": "phase_1_complete",
    "expected_processes": 4,
    "arrived_processes": 4,
    "generation": 1
  },
  "all_arrived": true,
  "next_generation": 2
}
```
