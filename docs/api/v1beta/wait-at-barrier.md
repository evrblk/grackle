---
title: WaitAtBarrier
type: docs
layout: grackle
---

# WaitAtBarrier

Blocks until the barrier releases for the requested generation (`all_arrived: true`) or until
`timeout_seconds` elapses (`timed_out: true`). Many peers can wait on the same generation at
once; they are all released together when the last expected peer arrives.

`WaitAtBarrier` does **not** register an arrival — a peer that needs to both contribute and wait
must call `ArriveAtBarrier` first.

Safe to retry — a timed-out caller can simply call again; late arrivals continue to accumulate 
in the background.

## Request

* `timeout_seconds` tells the server how long to wait for all participants to arrive before
giving up. 
* The call blocks server-side, so set client/RPC timeouts comfortably above `timeout_seconds`.
* `expected_generation` is the cycle the caller is waiting on; specifying a stale generation
  returns immediately with the already-released state.

```json
{
  "namespace_name": "pipelines",
  "barrier_name": "phase_1_complete",
  "expected_generation": 1,
  "timeout_seconds": 300
}
```

## Response 

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the barrier does not exist.

__Released before timeout:__

```json
{
  "barrier": {
    "name": "phase_1_complete",
    "expected_processes": 4,
    "arrived_processes": 4,
    "generation": 1
  },
  "all_arrived": true,
  "next_generation": 2,
  "timed_out": false
}
```

__Timeout fired first:__

```json
{
  "barrier": {
    "name": "phase_1_complete",
    "expected_processes": 4,
    "arrived_processes": 3,
    "generation": 1
  },
  "all_arrived": false,
  "next_generation": 2,
  "timed_out": true
}
```
