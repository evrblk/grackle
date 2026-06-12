---
title: RevokeLockLease
type: docs
layout: grackle
---

# RevokeLockLease

Releases every lock held by the lease and deletes the lease. This is the typical "process is
shutting down cleanly" call.

If a process crashes without calling this, the lease's TTL acts as the backstop — GC reaps it
and releases the holders.

Safe to retry — a second revoke on a deleted lease just returns `NotFound`.

## Request

```json
{
  "namespace_name": "UserObjects",
  "lease_id": "ll_NfKKeiPbP18NFeU3lLGrRWWgDJRB"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the lease does not exist.

```json
{}
```
