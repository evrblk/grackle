# CreateWaitGroup

Creates a new wait group.

You do not need to know all `job_id` values upfront — only the total count. A single wait group
can have millions of jobs.

Safe to retry — duplicate calls fail with `AlreadyExists`.

## Request

* `counter` is the total number of jobs the group is waiting for; it can be grown later 
  with `AddJobsToWaitGroup`.
* `expires_at` is an absolute timestamp after which the group is reaped by GC regardless of completion.
* Set `expires_at` to a value comfortably past the expected completion time. It can be pushed out
  later with [UpdateWaitGroup](/docs/api/v1beta/update-wait-group.md) if a batch needs more time.
* `metadata` is an optional, opaque map of string key/value pairs stored alongside the wait group —
  see [Metadata](/docs/api-overview.md#metadata).

```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12",
  "description": "Daily ETL batch",
  "counter": 100,
  "expires_at": 1718236800000000000,
  "metadata": {
    "pipeline": "etl-daily"
  }
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `AlreadyExists` if a wait group with the same name exists in the namespace.
* Returns `ResourceExhausted` if the namespace has reached its wait group quota.

```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "description": "Daily ETL batch",
    "counter": 100,
    "completed": 0,
    "version": 1,
    "created_at": 1718150400000000000,
    "updated_at": 1718150400000000000,
    "expires_at": 1718236800000000000,
    "metadata": {
      "pipeline": "etl-daily"
    }
  }
}
```
