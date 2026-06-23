# UpdateSemaphore

Updates a semaphore's `description` and/or `permits`. Permits can be raised freely; it can only be
lowered down to the current `active_holds`.

Safe to retry.

## Request

* `metadata` is an optional, opaque map of string key/value pairs stored alongside the semaphore —
  see [Metadata](/docs/api-overview.md#metadata).
* `expected_version` enables optimistic locking: the update is applied only if it equals the
  semaphore's current `version`. See [Updates](/docs/api-overview.md#updates).

```json
{
  "namespace_name": "third_parties",
  "semaphore_name": "partner_1",
  "description": "Partner 1 API (raised capacity)",
  "permits": 30,
  "expected_version": 1,
  "metadata": {
    "vendor": "partner-1"
  }
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the semaphore does not exist.
* Returns `InvalidArgument` if `expected_version` does not match the semaphore's current `version`.
* Returns `InvalidArgument` if the new `permits` is below the current `active_holds` (after
  pruning expired holders).
* Expired holders are pruned during the call so a stale `active_holds` cannot block a legitimate
  shrink.
* `last_activity_at` is not affected by updates — it only advances on `AcquireSemaphore` /
  `ReleaseSemaphore`.

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
    "updated_at": 1695826239925421000,
    "last_activity_at": 1695826239671432000,
    "metadata": {
      "vendor": "partner-1"
    }
  }
}
```
