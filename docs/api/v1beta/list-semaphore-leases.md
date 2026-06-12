---
title: ListSemaphoreLeases
type: docs
layout: grackle
---

# ListSemaphoreLeases

Lists semaphore unexpired leases in a namespace. Paginated.

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

```json
{
  "leases": [
    {
      "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
      "process_id": "host-123/pid-4567",
      "created_at": 1695826239671432000,
      "expires_at": 1695826299671432000
    }
  ],
  "next_pagination_token": "",
  "previous_pagination_token": ""
}
```
