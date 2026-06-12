---
title: ListNamespaces
type: docs
layout: grackle
---

# ListNamespaces

Lists all namespaces for the account. Paginated: leave `pagination_token` empty for the first
page; a non-empty `next_pagination_token` in the response indicates more pages are available.

Read-only and safe to retry.

## Request

* Leave `pagination_token` empty for the first page.
* `limit` sets the number of entries per page.

```json
{
  "pagination_token": "",
  "limit": 100
})
```

## Response

* Non-empty `next_pagination_token` indicates more pages are available.

```json
{
  "namespaces": [
    {
      "name": "UserObjects",
      "description": "Per-user objects",
      "version": 1,
      "created_at": 1695826239671432000,
      "updated_at": 1695826239671432000
    }
  ],
  "next_pagination_token": "bXlsb25ndG9rZW4yCg==",
  "previous_pagination_token": ""
}
```
