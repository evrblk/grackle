# AcquireSemaphore

Attempts to acquire `weight` permits on a semaphore under the caller's lease. The acquire is 
all-or-none: if `weight: 5` is requested but only 3 permits are available, the call waits up 
to the timeout for the missing 2 to free up.

Re-acquiring the same semaphore under the same `lease_id` updates the existing holder's weight
(subject to the permit cap) and refreshes its expiration to the lease's `expires_at`. It does not
create a second holder.

## Request

* `timeout_seconds` tells the server how long to wait for permits to free up before giving up.
* The call blocks server-side, so set client/RPC timeouts comfortably above `timeout_seconds`.
* `metadata` is an optional, opaque map of string key/value pairs attached to this holder —
  see [Metadata](/docs/api-overview.md#metadata).

```json
{
  "namespace_name": "third_parties",
  "semaphore_name": "partner_1",
  "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
  "weight": 3,
  "timeout_seconds": 60,
  "metadata": {
    "host": "worker-7"
  }
}
```

## Response 

* Returns `NotFound` if the namespace does not exist.
* Either succeeds atomically with `success: true`, or returns `success: false` (without an error) 
  when permits are unavailable.
* Returns `NotFound` if the lease or the semaphore does not exist (or the lease has expired).
* Use `ListSemaphoreHolders` to see who currently holds permits when `success: false`.

__Success:__

```json
{
  "semaphore": {
    "name": "partner_1",
    "permits": 20,
    "active_holds": 3,
    "active_holders_count": 1,
    "version": 1,
    "created_at": 1695826239671432000,
    "updated_at": 1695826239671432000
  },
  "success": true
}
```

__No permits left:__

```json
{
  "semaphore": {
    "name": "partner_1",
    "permits": 20,
    "active_holds": 20,
    "active_holders_count": 7
  },
  "success": false
}
```
