# AcquireLock

Attempts to acquire a lock under the caller's lease. Locks do not need to be created upfront —
the first successful acquire creates the lock record.

Lock names are paths; an acquire can be blocked by an existing holder at an ancestor
or descendant path — see [Locks](/docs/locks.md) for the hierarchical compatibility rules.

Re-acquiring under the same `lease_id` is always allowed and just refreshes the holder's
`locked_at`.

## Request

* `exclusive: true` requests a write (exclusive) lock — only one holder.
* `exclusive: false` requests a read (shared) lock — many holders allowed concurrently.
* `timeout_seconds` tells the server how long to wait for an incompatible holder to release before
giving up. 
* The call blocks server-side, so set client/RPC timeouts comfortably above `timeout_seconds`.
* `metadata` is an optional, opaque map of string key/value pairs attached to this lock holder —
  see [Metadata](/docs/api-overview.md#metadata).

```json
{
  "namespace_name": "UserObjects",
  "lock_name": "users/123/profile",
  "exclusive": true,
  "lease_id": "ll_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
  "timeout_seconds": 60,
  "metadata": {
    "host": "node-1"
  }
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* The `outcome` enum reports the result: `ACQUIRE_OUTCOME_ACQUIRED` when the lease now holds the
lock, `ACQUIRE_OUTCOME_UNAVAILABLE` when a non-blocking attempt (`timeout_seconds: 0`) found it
held by someone else and returned without waiting, or `ACQUIRE_OUTCOME_TIMED_OUT` when the call
blocked until `timeout_seconds` elapsed without ever acquiring. The non-acquired outcomes return
(without an error) the current lock state so the caller can see who is holding it.
* Returns `NotFound` if the lease does not exist or has already expired.
* Returns `ResourceExhausted` if creating a new lock would exceed the namespace's lock quota.

__Success:__

```json
{
  "lock": {
    "name": "users/123/profile",
    "state": "EXCLUSIVE_LOCKED",
    "locked_at": 1695826239671432000,
    "last_activity_at": 1695826239671432000,
    "lock_holders": [
      {
        "lease_id": "ll_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
        "locked_at": 1695826239671432000,
        "metadata": {
          "host": "node-1"
        }
      }
    ]
  },
  "outcome": "ACQUIRE_OUTCOME_ACQUIRED"
}
```

__Held by someone else:__

```json
{
  "lock": {
    "name": "users/123/profile",
    "state": "EXCLUSIVE_LOCKED",
    "locked_at": 1695826200000000000,
    "last_activity_at": 1695826200000000000,
    "lock_holders": [
      {
        "lease_id": "ll_qB7XwYzAaaaaaaaaaaaaaaaaaaaa",
        "locked_at": 1695826200000000000,
        "metadata": {
          "host": "node-2"
        }
      }
    ]
  },
  "outcome": "ACQUIRE_OUTCOME_TIMED_OUT"
}
```
