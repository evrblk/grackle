# ReleaseLock

Removes this lease from a lock's holder list. The lock becomes `UNLOCKED` only when the last
holder is gone. Releasing a lock the lease does not hold is a no-op (returns the lock unchanged,
no error). To release every lock held by a lease in one shot, use `RevokeLockLease`. To forcibly 
unlock regardless of holders, use `DeleteLock`.

Safe to retry.

## Request

```json
{
  "namespace_name": "UserObjects",
  "lock_name": "users/123/profile",
  "lease_id": "ll_NfKKeiPbP18NFeU3lLGrRWWgDJRB"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the lease does not exist.

```json
{
  "lock": {
    "name": "users/123/profile",
    "state": "UNLOCKED",
    "locked_at": 0,
    "lock_holders": []
  }
}
```
