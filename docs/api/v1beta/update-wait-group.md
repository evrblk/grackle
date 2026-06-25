# UpdateWaitGroup

Updates a wait group's mutable attributes: its `description`, its `counter`, its `expires_at`
deadline, its `delete_after_finished_seconds` retention, and its `metadata`. The `name` and the
`completed_jobs` count are immutable. `counter` is the total number of jobs the group waits for; it
can be raised or lowered, but not below the current `completed_jobs` count.

Only `ACTIVE` wait groups can be updated. A group that has already `COMPLETED` or `EXPIRED` is
finished and is rejected with `InvalidArgument`.

Pushing `expires_at` further into the future delays when the group is marked `EXPIRED`. 
`metadata` is replaced wholesale — an update that omits `metadata` clears it. See
[Metadata](/docs/api-overview.md#metadata).

Safe to retry.

## Request

* `counter` is the new total number of jobs the group waits for. It may be raised or lowered, but
  not below the current `completed_jobs` count. Lowering it to exactly `completed_jobs` finishes the
  group as `COMPLETED`.
* `delete_after_finished_seconds` is the retention period kept after the group finishes before GC
  deletes it. Must not be negative.
* `expected_version` enables optimistic locking: the update is applied only if it equals the wait
  group's current `version`. See [Updates](/docs/api-overview.md#updates).

```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12",
  "description": "Daily ETL batch (extended)",
  "counter": 110,
  "expires_at": 1718323200000000000,
  "delete_after_finished_seconds": 3600,
  "expected_version": 1,
  "metadata": { "team": "data", "pipeline": "etl", "priority": "high" }
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the wait group does not exist.
* Returns `InvalidArgument` if the wait group is not `ACTIVE` (it has already completed or expired).
* Returns `InvalidArgument` if `expected_version` does not match the wait group's current `version`.
* Returns `InvalidArgument` if the new `counter` is below the current `completed_jobs` count.
* `last_activity_at` is not affected by updates — it only advances on `CompleteJobsFromWaitGroup`.

```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "description": "Daily ETL batch (extended)",
    "status": "ACTIVE",
    "counter": 110,
    "completed_jobs": 73,
    "version": 2,
    "created_at": 1718150400000000000,
    "updated_at": 1718150480000000000,
    "expires_at": 1718323200000000000,
    "delete_after_finished_seconds": 3600,
    "finished_at": 0,
    "last_activity_at": 1718150420000000000,
    "metadata": { "team": "data", "pipeline": "etl", "priority": "high" }
  }
}
```
