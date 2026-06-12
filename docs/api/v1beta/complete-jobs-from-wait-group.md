---
title: CompleteJobsFromWaitGroup
type: docs
layout: grackle
---

# CompleteJobsFromWaitGroup

Marks a batch of jobs as completed. Each `job_id` is recorded once; reporting the same id again
is a no-op for `completed` (the call is idempotent per `job_id`).

`job_id` values are unique within a single wait group. They are independent of `process_id` used
by lock/semaphore leases — see [Wait Groups](/docs/wait-groups).

## Request

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

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the wait group does not exist.

```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "counter": 110,
    "completed": 3,
    "version": 1,
    "created_at": 1718150400000000000,
    "updated_at": 1718150480000000000,
    "expires_at": 1718236800000000000
  }
}
```
