# ListWaitGroups

Lists all wait groups in a namespace. Paginated.

Read-only and safe to retry.

## Request

* Leave `pagination_token` empty for the first page.
* `limit` sets the number of entries per page.


```json
{
  "namespace_name": "pipelines",
  "pagination_token": "",
  "limit": 100
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Non-empty `next_pagination_token` indicates more pages are available.
* Returns the current `status` and `completed_jobs` of each wait group.
* `metadata` is the optional, opaque map stored with each wait group — see [Metadata](/docs/api-overview.md#metadata).

```json
{
  "wait_groups": [
    {
      "name": "batch_2026_06_12",
      "description": "Daily ETL batch",
      "status": "ACTIVE",
      "counter": 110,
      "completed_jobs": 73,
      "version": 1,
      "created_at": 1718150400000000000,
      "updated_at": 1718150480000000000,
      "expires_at": 1718236800000000000,
      "delete_after_finished_seconds": 3600,
      "finished_at": 0,
      "last_activity_at": 1718150480000000000,
      "metadata": {
        "pipeline": "etl-daily"
      }
    }
  ],
  "next_pagination_token": "",
  "previous_pagination_token": ""
}
```
