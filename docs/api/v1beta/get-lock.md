# GetLock

Fetches a lock by name.

Read-only and safe to retry.

## Request

```json
{
  "namespace_name": "UserObjects",
  "lock_name": "users/123/profile"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Always returns a value, even if the lock has never been acquired — in that
case the response shows `state: UNLOCKED` and an empty holders list.

__Currently held:__

```json
{
  "lock": {
    "name": "users/123/profile",
    "state": "EXCLUSIVE_LOCKED",
    "locked_at": 1695826239671432000,
    "lock_holders": [
      {
        "lease_id": "ll_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
        "locked_at": 1695826239671432000
      }
    ]
  }
}
```

__Not held:__

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
