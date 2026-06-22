# UpdateWaitGroup

Updates a wait group's mutable attributes: its `description`, its `expires_at` deadline, and its
`metadata`. The `name`, `counter`, and `completed` count are immutable — grow the counter with
[AddJobsToWaitGroup](/docs/api/v1beta/add-jobs-to-wait-group.md) instead.

Pushing `expires_at` further into the future extends the group's lifetime before garbage
collection reaps it; the expiration schedule is reconciled atomically as part of the update.
`metadata` is replaced wholesale — an update that omits `metadata` clears it. See
[Metadata](/docs/api-overview.md#metadata).

Safe to retry.

## Request

```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12",
  "description": "Daily ETL batch (extended)",
  "expires_at": 1718323200000000000,
  "metadata": { "team": "data", "pipeline": "etl", "priority": "high" }
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the wait group does not exist.

```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "description": "Daily ETL batch (extended)",
    "counter": 110,
    "completed": 73,
    "version": 2,
    "created_at": 1718150400000000000,
    "updated_at": 1718150480000000000,
    "expires_at": 1718323200000000000,
    "metadata": { "team": "data", "pipeline": "etl", "priority": "high" }
  }
}
```
