---
title: CreateSemaphore
type: docs
layout: grackle
---

# CreateSemaphore

Creates a new semaphore in a namespace.

Safe to retry — duplicate calls fail with `AlreadyExists` rather than creating two.

## Request

* `permits` is the total capacity — the upper bound on how many units of the 
  resource may be held at once.

```json
{
  "namespace_name": "third_parties",
  "semaphore_name": "partner_1",
  "description": "Partner 1 API",
  "permits": 20
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `AlreadyExists` if a semaphore with the same name exists in the namespace.
* Returns `ResourceExhausted` if the namespace has reached its semaphore quota.

```json
{
  "semaphore": {
    "name": "partner_1",
    "description": "Partner 1 API",
    "permits": 20,
    "active_holds": 0,
    "active_holders_count": 0,
    "version": 1,
    "created_at": 1695826239671432000,
    "updated_at": 1695826239671432000
  }
}
```
