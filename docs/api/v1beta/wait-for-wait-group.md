---
title: WaitForWaitGroup
type: docs
layout: grackle
---

# WaitForWaitGroup

Blocks until the wait group is complete (`completed >= counter`) or until `timeout_seconds`
elapses. Many callers can wait on the same group at once; all of them are released together.

Safe to retry — a timed-out caller can simply call again; the group continues to make progress
in the background.

## Request

* The call blocks server-side, so set client/RPC timeouts comfortably above `timeout_seconds`.

```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12",
  "timeout_seconds": 300
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the wait group does not exist.
* `completed` and `timed_out` are mutually exclusive flags on the response — `timed_out: true`
  means the deadline fired first; `completed: true` means the group reached the threshold.

__Completed before timeout:__

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

__Timeout fired first:__

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
