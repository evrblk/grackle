# CreateBarrier

Creates a new barrier in a namespace.

Safe to retry — duplicate calls fail with `AlreadyExists`.

## Request

* `expected_processes` is how many peers must arrive before the barrier releases. It must be greater
  than 0.
* `delete_inactive_after_seconds` is the inactivity window after which the barrier is auto-deleted:
  garbage collection removes the barrier and its participants once
  `last_activity_at + delete_inactive_after_seconds` has passed. Every activity (creation and each
  `ArriveAtBarrier`) advances `last_activity_at` and pushes the deletion out. It must be greater
  than 0. Barriers do not expire on an absolute deadline.
* `metadata` is an optional, opaque map of string key/value pairs stored alongside the barrier —
  see [Metadata](/docs/api-overview.md#metadata).

```json
{
  "namespace_name": "pipelines",
  "barrier_name": "phase_1_complete",
  "description": "End of map phase",
  "expected_processes": 4,
  "delete_inactive_after_seconds": 3600,
  "metadata": {
    "phase": "map"
  }
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `AlreadyExists` if a barrier with the same name exists in the namespace.
* Returns `ResourceExhausted` if the namespace has reached its barrier quota.

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
    "updated_at": 1718150400000000000,
    "last_activity_at": 1718150400000000000,
    "delete_inactive_after_seconds": 3600,
    "metadata": {
      "phase": "map"
    }
  }
}
```
