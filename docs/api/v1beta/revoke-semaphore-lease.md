---
title: RevokeSemaphoreLease
type: docs
layout: grackle
---

# RevokeSemaphoreLease

Releases every semaphore held by the lease and deletes the lease. This is the typical "process is
shutting down cleanly" call.

If a process crashes without calling this, the lease's TTL acts as the backstop — GC reaps it
and releases the holders.

Safe to retry — a second revoke on a deleted lease just returns `NotFound`.

## Request

```json
{
  "namespace_name": "third_parties",
  "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the lease does not exist.

```json
{}
```
