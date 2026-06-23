# ReleaseSemaphore

Releases this lease's hold on a semaphore, freeing its permits. Releasing a semaphore the lease
does not hold is a no-op (returns the semaphore unchanged, no error). To release every semaphore 
held by a lease in one shot, use `RevokeSemaphoreLease`.

Safe to retry.

## Request

```json
{
  "namespace_name": "third_parties",
  "semaphore_name": "partner_1",
  "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB"
}
```

# Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the lease or the semaphore does not exist.

```json
{
  "semaphore": {
    "name": "partner_1",
    "permits": 20,
    "active_holds": 0,
    "active_holders_count": 0,
    "version": 1,
    "created_at": 1695826239671432000,
    "updated_at": 1695826239671432000,
    "last_activity_at": 1695826239671432000
  }
}
```
