# RefreshSemaphoreLease

Extends a semaphore lease's TTL to `now + ttl_seconds` and propagates the new `expires_at` to
every `SemaphoreHolder` owned by the lease. This is the heartbeat call that keeps a long-running
worker's permits from being reaped. Issue refreshes well before `expires_at` to absorb 
network/clock jitter.

Safe to retry - will extend the lease even further.

## Request

```json
{
  "namespace_name": "third_parties",
  "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
  "ttl_seconds": 60
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the lease does not exist.
* If the lease has already expired by call time, the server revokes it (releases all its holders)
  and returns `NotFound` — there is no way to resurrect an expired lease.

```json
{
  "lease": {
    "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
    "process_id": "host-123/pid-4567",
    "created_at": 1695826239671432000,
    "expires_at": 1695826359671432000
  }
}
```
