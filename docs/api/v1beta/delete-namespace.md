---
title: DeleteNamespace
type: docs
layout: grackle
---

# DeleteNamespace

Deletes a namespace and everything inside it — all locks, semaphores, wait groups, barriers, and
leases scoped to that namespace.

Destructive and not recoverable. There is no soft-delete.

Safe to retry.

## Request

```json
{
  "namespace_name": "UserObjects"
}
```

## Response

* Deleting a namespace that does not exist is a no-op — no error.

```json
{}
```
