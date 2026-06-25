# WaitForWaitGroup

Blocks until the wait group is complete (`completed_jobs == counter`) or until `timeout_seconds`
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
* The `outcome` enum reports why the call returned: `WAIT_GROUP_WAIT_OUTCOME_COMPLETED` means all
  jobs completed (its `status` is then `COMPLETED`), `WAIT_GROUP_WAIT_OUTCOME_EXPIRED` means the
  group's `expires_at` passed while still active (its `status` is then `EXPIRED`), and
  `WAIT_GROUP_WAIT_OUTCOME_TIMED_OUT` means `timeout_seconds` elapsed while the group was still
  active.

__Completed before timeout:__

```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "status": "COMPLETED",
    "counter": 110,
    "completed_jobs": 110,
    "expires_at": 1718236800000000000,
    "finished_at": 1718150700000000000,
    "last_activity_at": 1718150700000000000
  },
  "outcome": "WAIT_GROUP_WAIT_OUTCOME_COMPLETED"
}
```

__Timeout fired first:__

```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "status": "ACTIVE",
    "counter": 110,
    "completed_jobs": 73,
    "expires_at": 1718236800000000000,
    "last_activity_at": 1718150480000000000
  },
  "outcome": "WAIT_GROUP_WAIT_OUTCOME_TIMED_OUT"
}
```
