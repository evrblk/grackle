# Wait Groups

A Grackle **wait group** is a named counter for coordinating the completion of N parallel jobs.
The producer sets the total number of jobs it expects (and can revise it later); the workers report each job as
done; one or more observers block on `WaitForWaitGroup` until the group is fully complete. It is
the distributed equivalent of Go's `sync.WaitGroup`.

A wait group lives inside a **namespace** and is referenced by name. It must be created explicitly
with `CreateWaitGroup` before jobs can be added or completed.

## Core concepts

### Counter and completed jobs
A wait group has two numbers:

- `counter` — the total number of jobs the group is waiting for. Set at creation and changed later
  via `UpdateWaitGroup`; it can be raised or lowered, but not below `completed_jobs`.
- `completed_jobs` — the number of distinct jobs reported done via `CompleteJobsFromWaitGroup`.

The group is **complete** when `completed_jobs == counter`. You do not need to know all job IDs
upfront — only the total count. A wait group can have millions of jobs.

### Jobs and job IDs
Each job inside a wait group is identified by a **job ID** — a free-form string the caller
chooses. Grackle stores a job record per `job_id` it has seen completed, which makes
`CompleteJobsFromWaitGroup` **idempotent**: reporting the same `job_id` twice increments
`completed_jobs` only once. `job_id` values are unique within a wait group but unrelated across
groups.

Note that `job_id` is distinct from the `process_id` used by lock and semaphore leases: that one
identifies a worker that holds leases; this one identifies a unit of work within a single wait
group.

You can list completed jobs with `ListWaitGroupCompletedJobs` to see which jobs have checked in.

### Status
Every wait group has a `status`:

- `active` — the normal working state. The group is accepting completions and has neither
  completed nor expired yet. Only active wait groups can be updated with `UpdateWaitGroup`.
- `completed` — `completed_jobs` reached `counter`. The group is **finished**.
- `expired` — `expires_at` passed before the group completed. The group is **finished**.

`completed` and `expired` are terminal: a finished wait group never goes back to `active`, and it
can no longer be updated.

### Expiration, finishing, and cleanup
A wait group has an absolute `expires_at` set at creation. When that timestamp passes while the
group is still `active`, the group is marked `expired` — it is **not** deleted at that moment.
This is the backstop for crashed producers: a stalled group eventually leaves the `active` state
instead of lingering forever.

A wait group becomes **finished** in one of two ways: it `completed` (`completed_jobs == counter`)
or it `expired`. The moment it finishes is recorded as `finished_at`.

`delete_after_finished_seconds` controls automatic cleanup: a finished wait group (and all of its
job records) is deleted by GC once this many seconds have elapsed since `finished_at`. A value of
`0` means the group becomes eligible for deletion as soon as it finishes. This retention window
lets observers read the final state (for example, that the group `completed`) before it disappears.

`expires_at` is the wait group's own deadline; it is not tied to any lease. Wait groups do not use
leases.

### Waiting
`WaitForWaitGroup` is a blocking call. The server holds the request open until either
`completed >= counter` (returns `completed: true`) or `timeout_seconds` elapses (returns
`timed_out: true`). Many callers can wait on the same group at the same time; all of them are
released together when it completes.

### Metadata
A wait group carries an optional `metadata` map (string → string) set on `CreateWaitGroup` and
replaced by `UpdateWaitGroup`. Each completed job can also carry its own `metadata`, supplied on
`CompleteJobsFromWaitGroup` and returned by `ListWaitGroupCompletedJobs`. Metadata is opaque to
Grackle — see [Metadata](/docs/api-overview.md#metadata) for the shared semantics and limits.

## Example workflow

The producer creates a wait group for a batch of 100 jobs, gives it an absolute deadline, and asks
Grackle to keep it around for an hour (3600 seconds) after it finishes before deleting it.
(Assuming that a namespace `pipelines` already exists.)

CreateWaitGroupRequest:
```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12",
  "description": "Daily ETL batch",
  "counter": 100,
  "expires_at": 1718236800000000000,
  "delete_after_finished_seconds": 3600,
  "metadata": { "team": "data", "pipeline": "etl" }
}
```

CreateWaitGroupResponse:
```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "description": "Daily ETL batch",
    "status": "ACTIVE",
    "counter": 100,
    "completed_jobs": 0,
    "version": 1,
    "created_at": 1718150400000000000,
    "updated_at": 1718150400000000000,
    "expires_at": 1718236800000000000,
    "delete_after_finished_seconds": 3600,
    "finished_at": 0,
    "metadata": { "team": "data", "pipeline": "etl" }
  }
}
```

If the producer discovers more work after the fact, it revises the total with `UpdateWaitGroup`.
`counter` is the new total — not a delta — and `expected_version` must equal the group's current
`version` for the update to apply (see [Updates](/docs/api-overview.md#updates)):

UpdateWaitGroupRequest:
```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12",
  "counter": 110,
  "expected_version": 1
}
```

UpdateWaitGroupResponse:
```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "status": "ACTIVE",
    "counter": 110,
    "completed_jobs": 0,
    "version": 2,
    "expires_at": 1718236800000000000
  }
}
```

Workers report jobs as they finish. The call accepts a batch of `jobs`, each with a `job_id` and
optional `metadata`, and is idempotent — reporting the same id again is a no-op for
`completed_jobs` (the first reported metadata for a job id is the one that is kept).

CompleteJobsFromWaitGroupRequest:
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

CompleteJobsFromWaitGroupResponse:
```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "status": "ACTIVE",
    "counter": 110,
    "completed_jobs": 3,
    "expires_at": 1718236800000000000
  }
}
```

Meanwhile, an observer blocks on the group. The call returns as soon as `completed_jobs >= counter`
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
    "status": "COMPLETED",
    "counter": 110,
    "completed_jobs": 110,
    "expires_at": 1718236800000000000,
    "finished_at": 1718150700000000000
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
    "status": "ACTIVE",
    "counter": 110,
    "completed_jobs": 73,
    "expires_at": 1718236800000000000
  },
  "completed": false,
  "timed_out": true
}
```

A timed-out caller can simply call `WaitForWaitGroup` again — the group continues to make progress
in the background. Inspect the current state at any time with `GetWaitGroup`, or enumerate which
jobs have checked in with `ListWaitGroupCompletedJobs`.

`UpdateWaitGroup` revises a group's mutable attributes — its `description`, its `counter`, its
`expires_at` deadline, its `delete_after_finished_seconds` retention, and its `metadata`. The name
and `completed_jobs` count are immutable. Only `active` wait groups can be updated — a group
that has already `completed` or `expired` is finished and rejects updates with `InvalidArgument`.
Pushing `expires_at` further out is the way to give a slow batch more time before it expires; the
expiration schedule is reconciled atomically. The update is a full replacement, so send every
mutable field you want to keep — an update that omits `metadata` clears it, and one that omits
`counter` resets it. Each call is guarded by `expected_version` for optimistic concurrency — see
[Updates](/docs/api-overview.md#updates).

UpdateWaitGroupRequest:
```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12",
  "description": "Daily ETL batch (extended)",
  "counter": 110,
  "expires_at": 1718323200000000000,
  "delete_after_finished_seconds": 3600,
  "expected_version": 2,
  "metadata": { "team": "data", "pipeline": "etl", "priority": "high" }
}
```

UpdateWaitGroupResponse:
```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "description": "Daily ETL batch (extended)",
    "status": "ACTIVE",
    "counter": 110,
    "completed_jobs": 73,
    "version": 3,
    "expires_at": 1718323200000000000,
    "delete_after_finished_seconds": 3600,
    "metadata": { "team": "data", "pipeline": "etl", "priority": "high" }
  }
}
```

`DeleteWaitGroup` removes the group immediately (and its jobs are cleaned up asynchronously by GC).
You normally do not need it: a finished wait group — one that has `completed` or `expired` — is
deleted automatically once `delete_after_finished_seconds` have elapsed since it finished.

## When to use what

- **Wait group** — one or more producers fan out N jobs, one or more observers want to know when
  the fan-in is done. The counter is adjustable via `UpdateWaitGroup`; completion is idempotent per `job_id`.
- **Barrier** — N peers all need to meet at a synchronization point with no producer/observer
  asymmetry. Each participant calls `WaitAtBarrier` and is released when the configured number of
  participants have arrived.

Reach for a wait group when one party is fanning out work and another party (or parties) need to
know when the fan-in is done.

## API reference

* [CreateWaitGroup](/docs/api/v1beta/create-wait-group.md)
* [UpdateWaitGroup](/docs/api/v1beta/update-wait-group.md)
* [ListWaitGroups](/docs/api/v1beta/list-wait-groups.md)
* [GetWaitGroup](/docs/api/v1beta/get-wait-group.md)
* [DeleteWaitGroup](/docs/api/v1beta/delete-wait-group.md)
* [CompleteJobsFromWaitGroup](/docs/api/v1beta/complete-jobs-from-wait-group.md)
* [ListWaitGroupCompletedJobs](/docs/api/v1beta/list-wait-group-completed-jobs.md)
* [WaitForWaitGroup](/docs/api/v1beta/wait-for-wait-group.md)
