# Locks

A Grackle **lock** is a named read/write lock that mediates access to a resource. It lives inside a
**namespace** and is referenced by name. A lock can be held in one of two modes: **shared** (read)
by any number of holders at once, or **exclusive** (write) by a single holder. Clients
**acquire** the lock in one of these modes and **release** it when done.

Locks do not need to be created upfront. The first successful `AcquireLock` creates the lock
record; once the last holder releases (or expires) the lock is fully unlocked. `GetLock` on a name
that has never been acquired returns an unlocked lock.

## Core concepts

### Lock state
A lock is always in one of three states:

- `UNLOCKED` — no active holders.
- `SHARED_LOCKED` — one or more holders, all of them shared (readers).
- `EXCLUSIVE_LOCKED` — exactly one holder, holding the lock exclusively (a writer).

A lock used as a plain mutex is simply one that is only ever acquired in `exclusive` mode.

### Holders
Each successful acquire adds a **holder** record to the lock, carrying `lease_id` and `locked_at`.
A shared lock can have many holders; an exclusive lock has exactly one. `GetLock` returns the
current holder list so a caller that failed to acquire can see who is holding it.

### Hierarchical names
Lock names are paths separated by `/` (e.g. `users/123/profile`). Grackle treats them as a
hierarchy:

- An **exclusive** lock at any path blocks acquires on its ancestors and descendants.
- A **shared** lock at any path blocks **exclusive** acquires on its ancestors and descendants, but
  allows **shared** acquires anywhere up or down the path.
- Sibling paths are independent. Locks on `users/123` and `users/456` never interfere.

This is useful for guarding tree-shaped resources (filesystems, document trees, partitioned data)
without enumerating every leaf. Acquire from root to leaf and release from leaf to root to avoid
deadlocks.

### Leases
A lock acquisition is owned by a **lease**, not by the caller directly. A lease is a short-lived,
server-side TTL token created with `CreateLockLease`. All acquires made with the same `lease_id`
share its expiration. Calling `RefreshLockLease` extends the TTL of the lease and of every lock
holder it owns. `RevokeLockLease` releases every lock the lease holds, in one shot. If a lease
expires without being refreshed, its holders are reaped automatically — there are no stuck locks
when a process crashes.

A single lease may hold many locks at once. Leases are listed per namespace and can also be listed
by `process_id`. Lock leases and semaphore leases are independent and not interchangeable.

### Process IDs
A `process_id` is a free-form string the caller assigns to a lease at creation
(e.g. `"host-123/pid-4567"` or any opaque identifier of the work unit). Grackle does not interpret
it; it only stores it on the lease so callers can later enumerate leases by `process_id` to find
"what does this worker currently hold?" The acquire/release surface does **not** take `process_id`
— it takes `lease_id`.

## Example workflow

Process `host-123/pid-4567` creates a lease. `ttl_seconds` is added to "now" server-side.
(Assuming that a namespace `documents` already exists.)

CreateLockLeaseRequest:
```json
{
  "namespace_name": "documents",
  "process_id": "host-123/pid-4567",
  "ttl_seconds": 60,
}
```

CreateLockLeaseResponse:
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

Then the process tries to acquire a lock under that lease. `exclusive: true` requests a write
lock; `exclusive: false` requests a read (shared) lock. `timeout_seconds` tells how long the call
should wait if the lock is not immediately compatible. The acquire succeeds if the lock is
currently unlocked, already held in a compatible mode, or held by the **same lease** (re-acquiring
your own lock is always allowed and just refreshes `locked_at`). Otherwise `success: false` is
returned with no error and the current state of the lock.

AcquireLockRequest:
```json
{
  "namespace_name": "documents",
  "lock_name": "users/123/profile",
  "exclusive": true,
  "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
  "timeout_seconds": 60,
}
```

AcquireLockResponse (success):
```json
{
  "lock": {
    "name": "users/123/profile",
    "state": "EXCLUSIVE_LOCKED",
    "locked_at": 1695826239671432000,
    "lock_holders": [
      {
        "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
        "locked_at": 1695826239671432000
      }
    ]
  },
  "success": true
}
```

AcquireLockResponse (already locked by someone else — not an error):
```json
{
  "lock": {
    "name": "users/123/profile",
    "state": "EXCLUSIVE_LOCKED",
    "locked_at": 1695826200000000000,
    "lock_holders": [
      {
        "lease_id": "ls_qB7XwYzAaaaaaaaaaaaaaaaaaaaa",
        "locked_at": 1695826200000000000
      }
    ]
  },
  "success": false
}
```

When the process is done it should release the lock.

ReleaseLockRequest:
```json
{
  "namespace_name": "documents",
  "lock_name": "users/123/profile",
  "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB"
}
```

ReleaseLockResponse:
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

For a shared lock, the same `AcquireLock` call with `exclusive: false` succeeds for many leases at
once:

AcquireLockResponse:
```json
{
  "lock": {
    "name": "users/123/profile",
    "state": "SHARED_LOCKED",
    "locked_at": 1695826239671432000,
    "lock_holders": [
      {
        "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
        "locked_at": 1695826239671432000
      },
      {
        "lease_id": "ls_qB7XwYzAaaaaaaaaaaaaaaaaaaaa",
        "locked_at": 1695826240120000000
      }
    ]
  },
  "success": true
}
```

`ReleaseLock` removes this lease from the holders. The lock becomes `UNLOCKED` only when the last
holder is gone. Releasing a lock the lease does not hold is a no-op (returns the lock unchanged,
no error). `DeleteLock` removes the lock record entirely regardless of state — use it to forcibly
unlock something stuck.

## Working with leases

A process should create and maintain one or multiple leases while it is alive and use these leases
to hold locks. On startup the process calls `CreateLockLease` with some TTL, e.g. 60s. Then
periodically on a heartbeat it calls `RefreshLockLease` to extend TTL and signal that it is still
alive. In the meantime this lease can be used to call `AcquireLock` and `ReleaseLock`. On shutdown
the process calls `RevokeLockLease` and everything that it held will be released. If the process
crashes everything will be released as well when TTL of its lease is over.

Alternatively, a process can create a lease every time it needs to acquire a lock and set TTL
to the time it expects to work with that lock. Keep in mind that lease TTL starts counting right
after a lease is created, but an acquisition might not happen immediately if the lock is held
incompatibly.

## Hierarchical Lock Semantics

An exclusive lock at any level in the hierarchy grants exclusive access to that path and all its 
descendants. No other locks (shared or exclusive) can be acquired on the same path, any descendant 
paths, or any ancestor paths.

A shared lock allows multiple readers at the same level. Shared locks at a parent level allow shared
locks at descendant levels. Shared locks prevent exclusive locks from being acquired on the same path,
any descendant paths, or any ancestor paths.

| Existing Lock | Requested Lock | Result | Reasoning |
|---------------|----------------|--------|-----------|
| `a/b` (X) | `a/b/c` (S) | ❌ BLOCK | Parent exclusive lock prevents all descendant locks |
| `a/b` (X) | `a/b/c` (X) | ❌ BLOCK | Parent exclusive lock prevents all descendant locks |
| `a/b` (S) | `a/b/c` (S) | ✅ ALLOW | Parent shared lock allows descendant shared locks |
| `a/b` (S) | `a/b/c` (X) | ❌ BLOCK | Parent shared lock prevents descendant exclusive locks |
| `a/b/c` (X) | `a/b` (S) | ❌ BLOCK | Descendant exclusive lock prevents ancestor locks (intent conflict) |
| `a/b/c` (X) | `a/b` (X) | ❌ BLOCK | Descendant exclusive lock prevents ancestor locks (intent conflict) |
| `a/b/c` (S) | `a/b` (S) | ✅ ALLOW | Descendant shared lock allows ancestor shared locks |
| `a/b/c` (S) | `a/b` (X) | ❌ BLOCK | Descendant shared lock prevents ancestor exclusive locks |
| `a/b` (X) | `a/b` (S) | ❌ BLOCK | Same-level exclusive lock blocks all other locks |
| `a/b` (X) | `a/b` (X) | ❌ BLOCK | Same-level exclusive lock blocks all other locks |
| `a/b` (S) | `a/b` (S) | ✅ ALLOW | Multiple shared locks on same path are allowed |
| `a/b` (S) | `a/b` (X) | ❌ BLOCK | Shared lock prevents exclusive lock on same path |
| `a` (X) | `a/b` (S) | ❌ BLOCK | Parent exclusive lock prevents all descendant locks |
| `a` (X) | `a/b` (X) | ❌ BLOCK | Parent exclusive lock prevents all descendant locks |
| `a` (S) | `a/b` (S) | ✅ ALLOW | Parent shared lock allows descendant shared locks |
| `a` (S) | `a/b` (X) | ❌ BLOCK | Parent shared lock prevents descendant exclusive locks |
| `a/b/c/d` (X) | `a/b` (S) | ❌ BLOCK | Deep descendant exclusive lock prevents ancestor locks |
| `a/b/c/d` (X) | `a/b` (X) | ❌ BLOCK | Deep descendant exclusive lock prevents ancestor locks |
| `a/b/c/d` (S) | `a/b` (S) | ✅ ALLOW | Deep descendant shared lock allows ancestor shared locks |
| `a/b/c/d` (S) | `a/b` (X) | ❌ BLOCK | Deep descendant shared lock prevents ancestor exclusive locks |

---

Paths that are not in the same hierarchy (siblings or unrelated paths) should not interfere with each other:

| Existing Lock | Requested Lock | Result | Reasoning |
|---------------|----------------|--------|-----------|
| `a/b` (X) | `a/c` (S) | ✅ ALLOW | Sibling paths are independent |
| `a/b` (X) | `a/c` (X) | ✅ ALLOW | Sibling paths are independent |
| `a/b` (S) | `a/c` (S) | ✅ ALLOW | Sibling paths are independent |
| `a/b` (S) | `a/c` (X) | ✅ ALLOW | Sibling paths are independent |
| `a/b/c` (X) | `a/d/e` (X) | ✅ ALLOW | Sibling paths are independent |

## API reference

* [AcquireLock](/docs/api/v1beta/acquire-lock.md)
* [ReleaseLock](/docs/api/v1beta/release-lock.md)
* [GetLock](/docs/api/v1beta/get-lock.md)
* [DeleteLock](/docs/api/v1beta/delete-lock.md)
* [ListLocks](/docs/api/v1beta/list-locks.md)
* [CreateLockLease](/docs/api/v1beta/create-lock-lease.md)
* [RevokeLockLease](/docs/api/v1beta/revoke-lock-lease.md)
* [RefreshLockLease](/docs/api/v1beta/refresh-lock-lease.md)
* [ListLockLeases](/docs/api/v1beta/list-lock-leases.md)
* [GetLockLease](/docs/api/v1beta/get-lock-lease.md)
