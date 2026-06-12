# ListSemaphores

Lists all semaphores in a namespace. Paginated.

Read-only and safe to retry.

## Request

* Leave `pagination_token` empty for the first page.
* `limit` sets the number of entries per page.

```json
{
  "namespace_name": "third_parties",
  "pagination_token": "",
  "limit": 100
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Non-empty `next_pagination_token` indicates more pages are available.
* Returns current values of `active_holds` and `active_holders_count`.

```json
{
  "semaphores": [
    {
      "name": "partner_1",
      "description": "Partner 1 API",
      "permits": 20,
      "active_holds": 3,
      "active_holders_count": 1,
      "version": 1,
      "created_at": 1695826239671432000,
      "updated_at": 1695826239671432000
    }
  ],
  "next_pagination_token": "bXlsb25ndG9rZW4yCg==",
  "previous_pagination_token": ""
}
```
