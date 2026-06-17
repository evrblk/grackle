# Barriers

A Grackle **barrier** is a named N-party rendezvous point. A fixed number of peers agree to
synchronize at it: each one arrives, then waits, and they are all released together once the last
one has arrived. It is the distributed equivalent of a `pthread_barrier` or Java's
`CyclicBarrier` — useful when several workers need to finish a phase before any of them moves on
to the next.

A barrier lives inside a **namespace** and is referenced by name. It must be created explicitly
with `CreateBarrier` before processes can arrive or wait on it.

## Core concepts

### Expected and arrived processes
A barrier has two numbers:

- `expected_processes` — how many distinct peers must arrive before the barrier releases. Set at
  creation and changeable via `UpdateBarrier`.
- `arrived_processes` — how many distinct peers have arrived so far in the current generation.

The barrier **releases** when `arrived_processes == expected_processes`. Unlike a wait group there
is no producer/observer split — every participant both contributes to and waits on the same
barrier.

### Process IDs
Each participant identifies itself with a `process_id` — a free-form string the caller
chooses. Grackle stores one participant record per `(generation, process_id)`, which makes
`ArriveAtBarrier` **idempotent**: a worker that retries its arrival call is counted only once per
generation. `process_id` values are scoped to a single barrier.

This `process_id` identifies a peer participating in the rendezvous; it is unrelated to the
`process_id` used by lock and semaphore leases.

You can enumerate who has arrived in a given generation with `ListBarrierParticipants`.

### Generations
A barrier is reusable. Each completed rendezvous bumps the barrier's `generation` counter, and the
next cycle starts fresh with `arrived_processes = 0`. Every `ArriveAtBarrier` and
`WaitAtBarrier` call carries an `expected_generation` field so a slow client can't accidentally
contribute to a later cycle, and so a fast client can wait specifically for the cycle it cares
about.

When the barrier releases, `WaitAtBarrier` returns the `next_generation` value the peers should
use for their next round.

### Expiration
A barrier has an absolute `expires_at` set at creation. After that timestamp the barrier and its
participant records are reaped by GC regardless of state. This is the backstop for crashed peers
— a half-arrived barrier cannot block waiters forever.

`expires_at` is the barrier's own deadline; it is not tied to any lease. Barriers do not use
leases.

### Arrive vs. Wait
There are two ways to interact with a barrier:

- `ArriveAtBarrier` — records the caller's arrival and returns **immediately**. The response
  carries the current barrier state and `all_arrived` so a caller that does not need to block can
  fire-and-go.
- `WaitAtBarrier` — blocks until the barrier releases for the requested generation (returns
  `all_arrived: true`) or `timeout_seconds` elapses (returns `timed_out: true`). The wait call
  does not register an arrival — peers usually call `ArriveAtBarrier` first, then
  `WaitAtBarrier`.

## Example workflow

The coordinator creates a barrier for 4 worker shards that need to meet at end-of-phase.
(Assuming that a namespace `pipelines` already exists.)

CreateBarrierRequest:
```json
{
  "namespace_name": "pipelines",
  "barrier_name": "phase_1_complete",
  "description": "End of map phase",
  "expected_processes": 4,
  "expires_at": 1718236800000000000
})
```

CreateBarrierResponse:
```json
{
  "barrier": {
    "name": "phase_1_complete",
    "description": "End of map phase",
    "expected_processes": 4,
    "arrived_processes": 0,
    "generation": 1,
    "version": 1,
    "created_at": 1718150400000000000,
    "updated_at": 1718150400000000000
  }
}
```

Each worker reports arrival with its own `process_id` and the current generation. The call is
non-blocking and idempotent — a retry with the same `(process_id, expected_generation)` is a
no-op.

ArriveAtBarrierRequest:
```json
{
  "namespace_name": "pipelines",
  "barrier_name": "phase_1_complete",
  "process_id": "shard-0",
  "expected_generation": 1
}
```

ArriveAtBarrierResponse (not yet released):
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

A worker that needs to block until the whole group has caught up calls `WaitAtBarrier`. Many
peers can wait on the same generation at once; they are all released together.

WaitAtBarrierRequest:
```json
{
  "namespace_name": "pipelines",
  "barrier_name": "phase_1_complete",
  "expected_generation": 1,
  "timeout_seconds": 300
}
```

WaitAtBarrierResponse (all peers arrived before timeout):
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

WaitAtBarrierResponse (timeout fired first):
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

A timed-out caller can simply call `WaitAtBarrier` again — late arrivals continue to accumulate
in the background. For the next phase, every peer uses `expected_generation: 1` from
`next_generation` and the cycle repeats.

Inspect the barrier at any time with `GetBarrier`, or enumerate who has arrived in a given
generation with `ListBarrierParticipants`. `DeleteBarrier` removes the barrier and its
participant records; one that reaches `expires_at` is deleted automatically.

## When to use what

- **Barrier** — N peers all need to meet at a synchronization point and continue together.
  Symmetric: every participant both contributes and waits. Reusable across phases via generations.
- **Wait group** — one or more producers fan out N jobs, one or more observers want to know when
  the fan-in is done. Asymmetric: producers grow the counter, workers report completions,
  observers block.

Reach for a barrier when peers are equals and need to march in lockstep through phases; reach for
a wait group when one party is fanning out work and another party (or parties) need to know when
the fan-in is done.

## API reference

* [CreateBarrier](/docs/api/v1beta/create-barrier.md)
* [ListBarriers](/docs/api/v1beta/list-barriers.md)
* [GetBarrier](/docs/api/v1beta/get-barrier.md)
* [DeleteBarrier](/docs/api/v1beta/delete-barrier.md)
* [UpdateBarrier](/docs/api/v1beta/update-barrier.md)
* [ArriveAtBarrier](/docs/api/v1beta/arrive-at-barrier.md)
* [WaitAtBarrier](/docs/api/v1beta/wait-at-barrier.md)
* [ListBarrierParticipants](/docs/api/v1beta/list-barrier-participants.md)
