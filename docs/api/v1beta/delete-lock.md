---
title: DeleteLock
type: docs
layout: grackle
---

# DeleteLock

Deletes a lock regardless of state. Use this to forcibly unlock a stuck lock — the holders
are dropped immediately. Under normal operation, prefer `ReleaseLock` (one lock) or 
`RevokeLockLease` (everything a lease holds) — reserve `DeleteLock` for cleanup scenarios.

Destructive: any active holders lose their lock without notification.

Safe to retry.

## Request

```json
{
  "namespace_name": "UserObjects",
  "lock_name": "users/123/profile"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Deleting a lock that does not exist is a no-op — no error.

```json
{}
```
