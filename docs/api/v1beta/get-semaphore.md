# GetSemaphore

Fetches a semaphore by name. Does not list holders themselves — use `ListSemaphoreHolders` for that.

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
* Returns `NotFound` if the semaphore does not exist.
* Returned `active_holds` and `active_holders_count` reflect any holders
whose lease has expired by the time of the call.

```json
{
  "semaphore": {
    "name": "partner_1",
    "description": "Partner 1 API",
    "permits": 20,
    "active_holds": 3,
    "active_holders_count": 1,
    "version": 1,
    "created_at": 1695826239671432000,
    "updated_at": 1695826239671432000
  }
}
```
