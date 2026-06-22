# ListLocks

Lists locks currently tracked in a namespace. Paginated. Only locks that have at least one holder
are stored; never-acquired (unlocked) names do not appear.

Read-only and safe to retry.

## Request

* Leave `pagination_token` empty for the first page.
* `limit` sets the number of entries per page.

```json
{
  "namespace_name": "UserObjects",
  "pagination_token": "",
  "limit": 100
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Non-empty `next_pagination_token` indicates more pages are available.
* Each holder's `metadata` is the optional, opaque map attached at acquire time — see [Metadata](/docs/api-overview.md#metadata).

```json
{
  "locks": [
    {
      "name": "users/123/profile",
      "state": "EXCLUSIVE_LOCKED",
      "locked_at": 1695826239671432000,
      "lock_holders": [
        {
          "lease_id": "ll_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
          "locked_at": 1695826239671432000,
          "metadata": {
            "host": "node-1"
          }
        }
      ]
    }
  ],
  "next_pagination_token": "",
  "previous_pagination_token": ""
}
```
