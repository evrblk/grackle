---
title: AddJobsToWaitGroup
type: docs
layout: grackle
---

# AddJobsToWaitGroup

Grows a wait group's `counter` by the given amount. Use this when the producer discovers more
work after creating the group. The counter can only grow — there is no API to decrease it.

**Not idempotent on retry**. Each call is additive — calling twice with `counter: 10` raises 
the total by 20. Producers should drive this from a deterministic place (e.g. after 
enqueueing N new jobs into a queue) or de-dupe themselves.

## Request

```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12",
  "counter": 10
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the wait group does not exist.
* Returns `ResourceExhausted` if the new total `counter` exceeds the per-group size limit.


```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "counter": 110,
    "completed": 12,
    "version": 1,
    "created_at": 1718150400000000000,
    "updated_at": 1718150420000000000,
    "expires_at": 1718236800000000000
  }
}
```
