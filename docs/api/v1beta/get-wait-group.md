# GetWaitGroup

Fetches a wait group by name. For blocking until completion, use `WaitForWaitGroup` 
instead of polling.

Read-only and safe to retry.

## Request

```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the wait group does not exist (including after `expires_at`).
* Returns the current `counter` and `completed`; the group is considered complete 
  when `completed >= counter`.
* `metadata` is the optional, opaque map stored with the wait group — see [Metadata](/docs/api-overview.md#metadata).

```json
{
  "wait_group": {
    "name": "batch_2026_06_12",
    "description": "Daily ETL batch",
    "counter": 110,
    "completed": 73,
    "version": 1,
    "created_at": 1718150400000000000,
    "updated_at": 1718150480000000000,
    "expires_at": 1718236800000000000,
    "metadata": {
      "pipeline": "etl-daily"
    }
  }
}
```
