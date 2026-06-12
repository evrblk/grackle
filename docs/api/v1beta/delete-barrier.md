---
title: DeleteBarrier
type: docs
layout: grackle
---

# DeleteBarrier

Deletes a barrier. Any in-flight `WaitAtBarrier` callers will see `NotFound`.

A barrier that reaches its `expires_at` is deleted automatically — explicit delete is only
needed for early cleanup.

Safe to retry.

## Request

```json
{
  "namespace_name": "pipelines",
  "barrier_name": "phase_1_complete"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Deleting a barrier that does not exist is a no-op — no error.

```json
{}
```
