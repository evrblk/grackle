# ListSemaphoreHolders

Lists current holders of a semaphore. Each holder carries the `lease_id` that grabbed the permits,
its `weight`, and when it acquired. Useful for diagnostics when an `AcquireSemaphore` returns
`success: false`. Paginated.

Read-only and safe to retry.

## Request

* Leave `pagination_token` empty for the first page.
* `limit` sets the number of entries per page.

```json
{
  "namespace_name": "third_parties",
  "semaphore_name": "partner_1",
  "pagination_token": "",
  "limit": 100
})
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the semaphore does not exist.
* Holders whose lease has already expired by call time are filtered out of the response.
* Non-empty `next_pagination_token` indicates more pages are available.

```json
{
  "holders": [
    {
      "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
      "weight": 3,
      "locked_at": 1695826239671432000
    },
    {
      "lease_id": "ls_qB7XwYzAaaaaaaaaaaaaaaaaaaaa",
      "weight": 2,
      "locked_at": 1695826240120000000
    }
  ],
  "next_pagination_token": "",
  "previous_pagination_token": ""
}
```
