# UpdateBarrier

Updates a barrier's `description` and/or `expected_processes`. Useful when the size of the peer
group changes between phases.

Lowering `expected_processes` below the current `arrived_processes` can cause the barrier to 
release immediately on the next call that touches it — change it between generations when possible.

Safe to retry.

## Request

```json
{
  "namespace_name": "pipelines",
  "barrier_name": "phase_1_complete",
  "description": "End of map phase (scaled)",
  "expected_processes": 6
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the barrier does not exist.

```json
{
  "barrier": {
    "name": "phase_1_complete",
    "description": "End of map phase (scaled)",
    "expected_processes": 6,
    "arrived_processes": 2,
    "generation": 1,
    "version": 2,
    "created_at": 1718150400000000000,
    "updated_at": 1718150460000000000
  }
}
```
