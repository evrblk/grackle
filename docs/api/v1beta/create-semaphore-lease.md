# CreateSemaphoreLease

Creates a new semaphore lease. The lease is a TTL token used by `AcquireSemaphore`/
`ReleaseSemaphore`; `expires_at` is computed as `now + ttl_seconds` server-side.

OK to retry — every call creates a new lease.

## Request

* `process_id` is a free-form string identifying the worker that owns the lease.
* The same `process_id` may own many leases. The server does not enforce uniqueness.

```json
{
  "namespace_name": "third_parties",
  "process_id": "host-123/pid-4567",
  "ttl_seconds": 60
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `ResourceExhausted` if the namespace has reached its lease quota.
* Lease lease IDs are server-generated; do not assume any format beyond opacity.

```json
{
  "lease": {
    "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
    "process_id": "host-123/pid-4567",
    "created_at": 1695826239671432000,
    "expires_at": 1695826299671432000
  }
}
```
