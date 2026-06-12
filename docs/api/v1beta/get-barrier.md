---
title: GetBarrier
type: docs
layout: grackle
---

# GetBarrier

Fetches a barrier by name. For blocking until release, use `WaitAtBarrier` instead of polling.

Read-only and safe to retry.

## Request

```json
{
  "namespace_name": "pipelines",
  "barrier_name": "phase_1_complete"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns the current `generation`, `arrived_processes`, and `expected_processes`.
* Returns `NotFound` if the barrier does not exist (including after `expires_at`).

```json
{
  "barrier": {
    "name": "phase_1_complete",
    "description": "End of map phase",
    "expected_processes": 4,
    "arrived_processes": 2,
    "generation": 1,
    "version": 1,
    "created_at": 1718150400000000000,
    "updated_at": 1718150420000000000
  }
}
```
