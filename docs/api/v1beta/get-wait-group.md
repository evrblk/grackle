# GetWaitGroup

Fetches a wait group by name. For blocking until completion, use `WaitForWaitGroup` 
instead of polling.

Read-only and safe to retry.

## Request

```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the wait group does not exist (including after it was deleted following
  `delete_after_finished_seconds`).
* Returns the current `counter` and `completed_jobs`; the group is considered complete
  when `completed_jobs >= counter`.
* `status` is one of `active`, `completed`, or `expired`. `finished_at` is the timestamp at which
  the group finished (completed or expired), or `0` while it is still active.
* `metadata` is the optional, opaque map stored with the wait group — see [Metadata](/docs/api-overview.md#metadata).

```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "description": "Daily ETL batch",
    "status": "ACTIVE",
    "counter": 110,
    "completed_jobs": 73,
    "version": 1,
    "created_at": 1718150400000000000,
    "updated_at": 1718150480000000000,
    "expires_at": 1718236800000000000,
    "delete_after_finished_seconds": 3600,
    "finished_at": 0,
    "metadata": {
      "pipeline": "etl-daily"
    }
  }
}
```
