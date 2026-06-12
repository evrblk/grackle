# Wait Groups

A Grackle **wait group** is a named counter for coordinating the completion of N parallel jobs.
The producer sets (or grows) the total number of jobs it expects; the workers report each job as
done; one or more observers block on `WaitForWaitGroup` until the group is fully complete. It is
the distributed equivalent of Go's `sync.WaitGroup`.

A wait group lives inside a **namespace** and is referenced by name. It must be created explicitly
with `CreateWaitGroup` before jobs can be added or completed.

## Core concepts

### Counter and completed
A wait group has two numbers:

- `counter` — the total number of jobs the group is waiting for. Set at creation and grown via
  `AddJobsToWaitGroup`. There is no API to decrease it.
- `completed` — the number of distinct jobs reported done via `CompleteJobsFromWaitGroup`.

The group is **complete** when `completed >= counter`. You do not need to know all job IDs upfront
— only the total count. A wait group can have millions of jobs.

### Jobs and job IDs
Each job inside a wait group is identified by a **job ID** — a free-form string the caller
chooses. Grackle stores a job record per `job_id` it has seen completed, which makes
`CompleteJobsFromWaitGroup` **idempotent**: reporting the same `job_id` twice increments
`completed` only once. `job_id` values are unique within a wait group but unrelated across groups.

Note that `job_id` is distinct from the `process_id` used by lock and semaphore leases: that one
identifies a worker that holds leases; this one identifies a unit of work within a single wait
group.

You can list completed jobs with `ListWaitGroupCompletedJobs` to see which jobs have checked in.

### Expiration
A wait group has an absolute `expires_at` set at creation. After that timestamp the group and its
jobs are reaped by GC regardless of whether `completed` ever reached `counter`. This is the
backstop for crashed producers — there are no orphaned wait groups.

`expires_at` is the wait group's own deadline; it is not tied to any lease. Wait groups do not use
leases.

### Waiting
`WaitForWaitGroup` is a blocking call. The server holds the request open until either
`completed >= counter` (returns `completed: true`) or `timeout_seconds` elapses (returns
`timed_out: true`). Many callers can wait on the same group at the same time; all of them are
released together when it completes.

## Example workflow

The producer creates a wait group for a batch of 100 jobs and gives it an absolute deadline.
(Assuming that a namespace `pipelines` already exists.)

CreateWaitGroupRequest:
```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12",
  "description": "Daily ETL batch",
  "counter": 100,
  "expires_at": 1718236800000000000
}
```

CreateWaitGroupResponse:
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
    "expires_at": 1718236800000000000
  }
}
```

If the producer discovers more work after the fact, it grows the counter:

AddJobsToWaitGroupRequest:
```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12",
  "counter": 10
}
```

AddJobsToWaitGroupResponse:
```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "counter": 110,
    "completed": 0,
    "expires_at": 1718236800000000000
  }
}
```

Workers report jobs as they finish. The call accepts a batch of `job_ids` and is idempotent —
reporting the same id again is a no-op for `completed`.

CompleteJobsFromWaitGroupRequest:
```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12",
  "job_ids": [
    "shard-0",
    "shard-1",
    "shard-2"
  ]
}
```

CompleteJobsFromWaitGroupResponse:
```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "counter": 110,
    "completed": 3,
    "expires_at": 1718236800000000000
  }
}
```

Meanwhile, an observer blocks on the group. The call returns as soon as `completed >= counter`
or the timeout elapses.

WaitForWaitGroupRequest:
```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12",
  "timeout_seconds": 300
}
```

WaitForWaitGroupResponse (group completed before timeout):
```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "counter": 110,
    "completed": 110,
    "expires_at": 1718236800000000000
  },
  "completed": true,
  "timed_out": false
}
```

WaitForWaitGroupResponse (timeout fired first):
```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "counter": 110,
    "completed": 73,
    "expires_at": 1718236800000000000
  },
  "completed": false,
  "timed_out": true
}
```

A timed-out caller can simply call `WaitForWaitGroup` again — the group continues to make progress
in the background. Inspect the current state at any time with `GetWaitGroup`, or enumerate which
jobs have checked in with `ListWaitGroupCompletedJobs`.

`DeleteWaitGroup` removes the group (and its jobs are cleaned up asynchronously by GC). A wait
group that reaches its `expires_at` is deleted automatically.

## When to use what

- **Wait group** — one or more producers fan out N jobs, one or more observers want to know when
  the fan-in is done. Counter only ever grows; completion is idempotent per `job_id`.
- **Barrier** — N peers all need to meet at a synchronization point with no producer/observer
  asymmetry. Each participant calls `WaitAtBarrier` and is released when the configured number of
  participants have arrived.

Reach for a wait group when one party is fanning out work and another party (or parties) need to
know when the fan-in is done.

## API reference

* [CreateWaitGroup](/docs/api/v1beta/create-wait-group.md)
* [ListWaitGroups](/docs/api/v1beta/list-wait-groups.md)
* [GetWaitGroup](/docs/api/v1beta/get-wait-group.md)
* [DeleteWaitGroup](/docs/api/v1beta/delete-wait-group.md)
* [AddJobsToWaitGroup](/docs/api/v1beta/add-jobs-to-wait-group.md)
* [CompleteJobsFromWaitGroup](/docs/api/v1beta/complete-jobs-from-wait-group.md)
* [ListWaitGroupCompletedJobs](/docs/api/v1beta/list-wait-group-completed-jobs.md)
* [WaitForWaitGroup](/docs/api/v1beta/wait-for-wait-group.md)
