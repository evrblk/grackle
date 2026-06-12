# ListBarriers

Lists all barriers in a namespace. Paginated.

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

```json
{
  "barriers": [
    {
      "name": "phase_1_complete",
      "description": "End of map phase",
      "expected_processes": 4,
      "arrived_processes": 2,
      "generation": 1,
      "version": 1,
      "created_at": 1718150400000000000,
      "updated_at": 1718150420000000000
    }
  ],
  "next_pagination_token": "",
  "previous_pagination_token": ""
}
```
