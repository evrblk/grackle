# CompleteJobsFromWaitGroup

Marks a batch of jobs as completed. Each entry in `jobs` has a `job_id` and an optional `metadata`
map. A `job_id` is recorded once; reporting the same id again is a no-op for `completed_jobs` (the
call is idempotent per `job_id`, and the metadata stored is the one from the first time the id was
reported).

When `completed_jobs` reaches `counter` the group becomes `completed` (a terminal `status`) and is
scheduled for deletion after its `delete_after_finished_seconds` retention window.

`job_id` values are unique within a single wait group. They are independent of `process_id` used
by lock/semaphore leases — see [Wait Groups](/docs/wait-groups.md). The optional per-job `metadata`
is opaque to Grackle and is returned by `ListWaitGroupCompletedJobs` — see
[Metadata](/docs/api-overview.md#metadata).

## Request

```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12",
  "jobs": [
    { "job_id": "shard-0", "metadata": { "worker": "worker-7" } },
    { "job_id": "shard-1" },
    { "job_id": "shard-2" }
  ]
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the wait group does not exist.
* Returns `InvalidArgument` if the call would push `completed_jobs` above `counter`.

```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "status": "ACTIVE",
    "counter": 110,
    "completed_jobs": 3,
    "version": 1,
    "created_at": 1718150400000000000,
    "updated_at": 1718150480000000000,
    "expires_at": 1718236800000000000,
    "delete_after_finished_seconds": 3600,
    "finished_at": 0
  }
}
```
