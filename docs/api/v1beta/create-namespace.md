---
title: CreateNamespace
type: docs
layout: grackle
---

# CreateNamespace

Creates a new namespace. Namespace names are unique per account. Locks, semaphores, wait groups,
and barriers all live inside a namespace and are scoped to it.

Safe to retry — duplicate calls fail with `AlreadyExists` rather than creating two namespaces.

## Request

```json
{
  "name": "UserObjects",
  "description": "Per-user objects"
}
```

## Response

* Returns `AlreadyExists` if a namespace with the same name exists.

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
