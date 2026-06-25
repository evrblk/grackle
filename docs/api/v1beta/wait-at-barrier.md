# WaitAtBarrier

Blocks until the barrier releases for the requested generation or until `timeout_seconds` elapses.
The `outcome` enum reports which happened: `BARRIER_WAIT_OUTCOME_TRIPPED` when all expected
processes arrived and the barrier advanced a generation, or `BARRIER_WAIT_OUTCOME_TIMED_OUT` when
`timeout_seconds` elapsed before the barrier tripped. Many peers can wait on the same generation at
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
* On `TRIPPED`, the next cycle's generation is simply `expected_generation + 1` — generations
  advance by exactly one per trip, so the caller computes it without a dedicated field.
  `barrier.generation` reports where the barrier actually is now (possibly further ahead if a
  later cohort has already tripped it again).

__Released before timeout__ (the barrier has advanced to generation 2 and `arrived_processes`
reset to 0):

```json
{
  "barrier": {
    "name": "phase_1_complete",
    "expected_processes": 4,
    "arrived_processes": 0,
    "generation": 2
  },
  "outcome": "BARRIER_WAIT_OUTCOME_TRIPPED"
}
```

__Timeout fired first__ (no trip, so the barrier is unchanged):

```json
{
  "barrier": {
    "name": "phase_1_complete",
    "expected_processes": 4,
    "arrived_processes": 3,
    "generation": 1
  },
  "outcome": "BARRIER_WAIT_OUTCOME_TIMED_OUT"
}
```
