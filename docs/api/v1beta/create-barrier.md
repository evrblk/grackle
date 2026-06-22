# CreateBarrier

Creates a new barrier in a namespace.

Safe to retry — duplicate calls fail with `AlreadyExists`.

## Request

* `expected_processes` is how many peers must arrive before the barrier releases.
* `expires_at` is an absolute timestamp after which the barrier and its participant records 
  are reaped by GC regardless of state.
* `metadata` is an optional, opaque map of string key/value pairs stored alongside the barrier —
  see [Metadata](/docs/api-overview.md#metadata).

```json
{
  "namespace_name": "pipelines",
  "barrier_name": "phase_1_complete",
  "description": "End of map phase",
  "expected_processes": 4,
  "expires_at": 1718236800000000000,
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
    "metadata": {
      "phase": "map"
    }
  }
}
```
