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
  "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
  "timeout_seconds": 60,
  "metadata": {
    "host": "node-1"
  }
}
```

## Response

The `outcome` enum reports the result: `ACQUIRE_OUTCOME_ACQUIRED` when the lease now holds the
lock, `ACQUIRE_OUTCOME_UNAVAILABLE` when a non-blocking attempt (`timeout_seconds: 0`) found it
held by someone else and returned without waiting, or `ACQUIRE_OUTCOME_TIMED_OUT` when the call
blocked until `timeout_seconds` elapsed without ever acquiring. The non-acquired outcomes return
(without an error) the current lock state so the caller can see who is holding it. 

When the outcome is not `ACQUIRE_OUTCOME_ACQUIRED`, the `reason` enum hints at *what* the acquire
was blocked on: `CONTENTION_REASON_PEER` (the lock itself is held in an incompatible mode),
`CONTENTION_REASON_ANCESTOR` (a lock on an ancestor path blocks it), or
`CONTENTION_REASON_DESCENDANT` (one or more locks on descendant paths block it). It is
`CONTENTION_REASON_UNSPECIFIED` when the lock was acquired.

`blocking_locks` carries the locks standing in the way — the blocking ancestor lock(s) or
blocking descendant locks, matching `reason` — so the caller can see who holds them. It is **empty
for `CONTENTION_REASON_PEER`**: there the conflicting lock is the one you are acquiring, already
returned in the top-level `lock` field, so it is not duplicated here. Both `reason` and
`blocking_locks` are **best-effort, point-in-time** diagnostics: they reflect the last acquire
attempt and may already be stale, so branch on `outcome`, not on these. `blocking_locks` is
**always capped at 50** (a contended descendant subtree may hold more), which keeps it cheap and
safe to read. It is empty when the lock was acquired.

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the lease does not exist or has already expired.
* Returns `ResourceExhausted` if creating a new lock would exceed the namespace's lock quota.

__Success:__

```json
{
  "lock": {
    "name": "users/123/profile",
    "state": "LOCK_STATE_EXCLUSIVE_LOCKED",
    "locked_at": 1695826239671432000,
    "last_activity_at": 1695826239671432000,
    "lock_holders": [
      {
        "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
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

The lock itself is held in an incompatible mode, so `reason` is `CONTENTION_REASON_PEER`. The
conflicting lock and its holder are already in the top-level `lock` field, so `blocking_locks` is
empty (omitted below).

```json
{
  "lock": {
    "name": "users/123/profile",
    "state": "LOCK_STATE_EXCLUSIVE_LOCKED",
    "locked_at": 1695826200000000000,
    "last_activity_at": 1695826200000000000,
    "lock_holders": [
      {
        "lease_id": "ls_qB7XwYzAaaaaaaaaaaaaaaaaaaaa",
        "locked_at": 1695826200000000000,
        "metadata": {
          "host": "node-2"
        }
      }
    ]
  },
  "outcome": "ACQUIRE_OUTCOME_TIMED_OUT",
  "reason": "CONTENTION_REASON_PEER"
}
```

__Blocked by an ancestor:__

A request for `users/123/profile` while an exclusive lock is held on the ancestor `users/123`
returns `reason` `CONTENTION_REASON_ANCESTOR`, with the blocking ancestor in `blocking_locks`. A
descendant conflict looks the same with `CONTENTION_REASON_DESCENDANT` and the blocking
descendant lock(s) instead.

```json
{
  "lock": {
    "name": "users/123/profile",
    "state": "LOCK_STATE_UNLOCKED",
    "locked_at": 0,
    "last_activity_at": 1695826200000000000,
    "lock_holders": []
  },
  "outcome": "ACQUIRE_OUTCOME_TIMED_OUT",
  "reason": "CONTENTION_REASON_ANCESTOR",
  "blocking_locks": [
    {
      "name": "users/123",
      "state": "LOCK_STATE_EXCLUSIVE_LOCKED",
      "locked_at": 1695826200000000000,
      "last_activity_at": 1695826200000000000,
      "lock_holders": [
        {
          "lease_id": "ls_qB7XwYzAaaaaaaaaaaaaaaaaaaaa",
          "locked_at": 1695826200000000000
        }
      ]
    }
  ]
}
```
