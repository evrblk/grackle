---
title: UpdateSemaphore
type: docs
layout: grackle
---

# UpdateSemaphore

Updates a semaphore's `description` and/or `permits`. Permits can be raised freely; it can only be
lowered down to the current `active_holds`.

Safe to retry.

## Request

```json
{
  "namespace_name": "third_parties",
  "semaphore_name": "partner_1",
  "description": "Partner 1 API (raised capacity)",
  "permits": 30
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the semaphore does not exist.
* Returns `InvalidArgument` if the new `permits` is below the current `active_holds` (after
  pruning expired holders).
* Expired holders are pruned during the call so a stale `active_holds` cannot block a legitimate
  shrink.

```json
{
  "semaphore": {
    "name": "partner_1",
    "description": "Partner 1 API (raised capacity)",
    "permits": 30,
    "active_holds": 3,
    "active_holders_count": 1,
    "version": 2,
    "created_at": 1695826239671432000,
    "updated_at": 1695826239925421000
  }
}
```
