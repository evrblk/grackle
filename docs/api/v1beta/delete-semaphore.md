---
title: DeleteSemaphore
type: docs
layout: grackle
---

# DeleteSemaphore

Deletes a semaphore. Any holders are dropped; clients holding the semaphore via a still-active
lease will not be notified.

Safe to retry.

## Request

```json
{
  "namespace_name": "third_parties",
  "semaphore_name": "partner_1"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Deleting a semaphore that does not exist is a no-op — no error.

```json
{}
```
