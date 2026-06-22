# ListWaitGroupCompletedJobs

Lists jobs that have been reported complete in a wait group. Paginated. Useful for diagnostics —
"which workers have not yet checked in?"

Read-only and safe to retry.

## Request

* Leave `pagination_token` empty for the first page.
* `limit` sets the number of entries per page.


```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12",
  "pagination_token": "",
  "limit": 100
}
```

## Response

* Returns `NotFound` if the wait group does not exist.
* Returns `NotFound` if the namespace does not exist.
* Only completed jobs appear — pending ones are not stored.
* Non-empty `next_pagination_token` indicates more pages are available.
* `metadata` is the optional, opaque map attached to each completed job — see [Metadata](/docs/api-overview.md#metadata).

```json
{
  "jobs": [
    {
      "job_id": "shard-0",
      "completed_at": 1718150410000000000,
      "metadata": {
        "host": "worker-7"
      }
    },
    {
      "job_id": "shard-1",
      "completed_at": 1718150412000000000,
      "metadata": {
        "host": "worker-9"
      }
    }
  ],
  "next_pagination_token": "",
  "previous_pagination_token": ""
}
```
