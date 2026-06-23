# UpdateBarrier

Updates a barrier's `description`, `expected_processes`, and/or `delete_inactive_after_seconds`.
Useful when the size of the peer group changes between phases — for example, shrinking the barrier
after a worker is decommissioned.

`expected_processes` must be greater than 0, and cannot be lowered below the current
`arrived_processes` (the request is rejected with `InvalidArgument`). Lowering it to *exactly*
`arrived_processes` satisfies the release condition, so the barrier trips as part of the update:
`arrived_processes` resets to 0 and `generation` is incremented, releasing anyone waiting on the old
generation. This is the safe way to release a barrier whose remaining peers will never arrive.

Safe to retry.

## Request

* `expected_processes` is the number of peers that must arrive before the barrier trips; it must be
  greater than 0.
* `delete_inactive_after_seconds` is the inactivity window before the barrier is auto-deleted; it
  must be greater than 0. Updating it reschedules the auto-deletion relative to the existing
  `last_activity_at` (an update is not itself activity, so it does not reset the inactivity clock).
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
  "delete_inactive_after_seconds": 3600,
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
* Returns `InvalidArgument` if `expected_processes` is 0.
* Returns `InvalidArgument` if `delete_inactive_after_seconds` is not greater than 0.
* Returns `InvalidArgument` if the new `expected_processes` is below the current `arrived_processes`.
* When the new `expected_processes` equals the current `arrived_processes`, the barrier trips:
  the returned `arrived_processes` is 0 and `generation` is incremented.
* `last_activity_at` is not affected by updates — it only advances on `ArriveAtBarrier`.

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
    "last_activity_at": 1718150420000000000,
    "delete_inactive_after_seconds": 3600,
    "metadata": {
      "phase": "map"
    }
  }
}
```
