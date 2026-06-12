# CreateLockLease

Creates a new lock lease. The lease is a TTL token used by `AcquireLock`/`ReleaseLock`;
`expires_at` is computed as `now + ttl_seconds` server-side. Lock leases and semaphore 
leases are independent and not interchangeable.

OK to retry — every call creates a new lease.

## Request

* `process_id` is a free-form string identifying the worker that owns the lease. The 
  same `process_id` may own many leases. The server does not enforce uniqueness.


```json
{
  "namespace_name": "UserObjects",
  "process_id": "host-123/pid-4567",
  "ttl_seconds": 60
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `ResourceExhausted` if the namespace has reached its lease quota.


```json
{
  "lease": {
    "lease_id": "ll_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
    "process_id": "host-123/pid-4567",
    "created_at": 1695826239671432000,
    "expires_at": 1695826299671432000
  }
}
```
