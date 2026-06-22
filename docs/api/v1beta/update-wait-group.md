# UpdateWaitGroup

Updates a wait group's mutable attributes: its `description`, its `counter`, its `expires_at`
deadline, and its `metadata`. The `name` and the `completed` count are immutable. `counter` is the
total number of jobs the group waits for; it can be raised or lowered, but not below the current
`completed` count.

Pushing `expires_at` further into the future extends the group's lifetime before garbage
collection reaps it; the expiration schedule is reconciled atomically as part of the update.
`metadata` is replaced wholesale — an update that omits `metadata` clears it. See
[Metadata](/docs/api-overview.md#metadata).

Safe to retry.

## Request

* `counter` is the new total number of jobs the group waits for. It may be raised or lowered, but
  not below the current `completed` count.
* `expected_version` enables optimistic locking: the update is applied only if it equals the wait
  group's current `version`. See [Updates](/docs/api-overview.md#updates).

```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12",
  "description": "Daily ETL batch (extended)",
  "counter": 110,
  "expires_at": 1718323200000000000,
  "expected_version": 1,
  "metadata": { "team": "data", "pipeline": "etl", "priority": "high" }
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the wait group does not exist.
* Returns `InvalidArgument` if `expected_version` does not match the wait group's current `version`.
* Returns `InvalidArgument` if the new `counter` is below the current `completed` count.

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
