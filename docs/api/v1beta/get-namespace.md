---
title: GetNamespace
type: docs
layout: grackle
---

# GetNamespace

Fetches a namespace by name.

Read-only and safe to retry.

## Request

```json
{
  "namespace_name": "UserObjects"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.

```json
{
  "namespace": {
    "name": "UserObjects",
    "description": "Per-user objects",
    "version": 1,
    "created_at": 1695826239671432000,
    "updated_at": 1695826239671432000
  }
}
```
