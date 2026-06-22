# UpdateBarrier

Updates a barrier's `description` and/or `expected_processes`. Useful when the size of the peer
group changes between phases.

Lowering `expected_processes` below the current `arrived_processes` can cause the barrier to 
release immediately on the next call that touches it — change it between generations when possible.

Safe to retry.

## Request

* `metadata` is an optional, opaque map of string key/value pairs stored alongside the barrier —
  see [Metadata](/docs/api-overview.md#metadata).
* `expected_version` enables optimistic locking: the update is applied only if it equals the
  barrier's current `version`. See [Updates](/docs/api-overview.md#updates).

```json
{
  "namespace_name": "pipelines",
  "barrier_name": "phase_1_complete",
  "description": "End of map phase (scaled)",
  "expected_processes": 6,
  "expected_version": 1,
  "metadata": {
    "phase": "map"
  }
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the barrier does not exist.
* Returns `InvalidArgument` if `expected_version` does not match the barrier's current `version`.
* Returns `InvalidArgument` if the new `expected_processes` is below the current `arrived_processes`.

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
    "updated_at": 1718150460000000000,
    "metadata": {
      "phase": "map"
    }
  }
}
```
