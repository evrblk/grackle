# Semaphores

A Grackle **semaphore** is a named counter that limits how many units of a resource can be in use
concurrently. It lives inside a **namespace** and is referenced by name. Each semaphore has a fixed
number of **permits** — the upper bound on how many units may be held at once. Clients **acquire**
some number of permits (a **weight**), and **release** them when done.

A semaphore must be created explicitly with `CreateSemaphore` before it can be acquired.

## Core concepts

### Permits
The total capacity of the semaphore, set at creation and changeable via `UpdateSemaphore`. The
invariant `active_holds <= permits` always holds. You cannot shrink `permits` below the current
`active_holds`.

### Weight
The number of permits a single acquisition consumes. `weight: 1` behaves like a classic counting
semaphore (one holder = one permit). `weight: 5` reserves 5 permits at once — useful when work items
have heterogeneous costs (e.g. a small job consumes 1, a large job consumes 5, against a pool of 20).
An acquisition is all-or-none. If `weight: 5`, but only 3 permits are available, then the acquisition
will not happen immediately, it will wait a specified period of time for enough permits to become 
available.

### Holders
Each successful acquire creates a **holder** record on the semaphore, carrying `lease_id`, `weight`,
and `locked_at`. `ListSemaphoreHolders` returns the active set. `active_holders_count` is the
number of holders; `active_holds` is the sum of their weights.

### Leases
A semaphore acquisition is owned by a **lease**, not by the caller directly. A lease is a
short-lived, server-side TTL token created with `CreateSemaphoreLease`. All acquires made with the
same `lease_id` share its expiration. Calling `RefreshSemaphoreLease` extends the TTL of the lease
and of every holder it owns. `RevokeSemaphoreLease` releases every semaphore the lease holds, in
one shot. If a lease expires without being refreshed, its holders are reaped automatically — there
are no stuck semaphores when a process crashes.

A single lease may hold many semaphores at once. Leases are listed per namespace and can also be
listed by `process_id`. Lock leases and semaphore leases are independent and not interchangeable.

### Process IDs
A `process_id` is a free-form string the caller assigns to a lease at creation
(e.g. `"host-123/pid-4567"` or any opaque identifier of the work unit). Grackle does not interpret
it; it only stores it on the lease so callers can later enumerate leases by `process_id` to find
"what does this worker currently hold?" The acquire/release surface does **not** take `process_id`
— it takes `lease_id`.

### Metadata
A semaphore carries an optional `metadata` map (string → string) set on `CreateSemaphore` and
replaced by `UpdateSemaphore`. Each holder can also attach its own `metadata` on `AcquireSemaphore`,
which is returned with the holder by `ListSemaphoreHolders`. Metadata is opaque to Grackle — see
[Metadata](/docs/api-overview.md#metadata) for the shared semantics and limits.

## Example workflow

Define a new semaphore. `permits` must be > 0. (Assuming that a namespace `third_parties` already 
exists):

CreateSemaphoreRequest:
```json
{
  "namespace_name": "third_parties",
  "semaphore_name": "partner_1",
  "description": "Partner 1 API",
  "permits": 20,
}
```

CreateSemaphoreResponse:
```json
{
  "semaphore": {
    "name": "partner_1",
    "description": "Partner 1 API",
    "permits": 20,
    "active_holds": 0,
    "active_holders_count": 0,
    "version": 1,
    "created_at": 1695826239671432000,
    "updated_at": 1695826239671432000
  }
}
```

Process `host-123/pid-4567` creates a lease. `ttl_seconds` is added to "now" server-side.

CreateSemaphoreLeaseRequest:
```json
{
  "namespace_name": "third_parties",
  "process_id": "host-123/pid-4567",
  "ttl_seconds": 60,
}
```

CreateSemaphoreLeaseResponse:
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

Then the process tries to acquire `weight` permits under the lease. The acquire succeeds only if
`active_holds + weight <= permits`; otherwise `success: false` is returned with no error and the
current state of the semaphore. `timeout_seconds` parameter tells how long should the call wait
if all necessary permits are not readily available.

AcquireSemaphoreRequest:
```json
{
  "namespace_name": "third_parties",
  "semaphore_name": "partner_1",
  "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
  "weight": 3,
  "timeout_seconds": 60,
}
```

AcquireSemaphoreResponse (success):
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
  },
  "success": true
}
```

AcquireSemaphoreResponse (no permits left — not an error):
```json
{
  "semaphore": {
    "name": "partner_1",
    "description": "Partner 1 API",
    "permits": 20,
    "active_holds": 20,
    "active_holders_count": 7,
    "version": 1,
    "created_at": 1695826239671432000,
    "updated_at": 1695826239671432000
  },
  "success": false
}
```

When the process is done with its work it should release the semaphore.

ReleaseSemaphoreRequest:
```json
{
  "namespace_name": "third_parties",
  "semaphore_name": "partner_1",
  "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB"
}
```

ReleaseSemaphoreResponse:
```json
{
  "semaphore": {
    "name": "partner_1",
    "description": "Partner 1 API",
    "permits": 20,
    "active_holds": 0,
    "active_holders_count": 0,
    "version": 1,
    "created_at": 1695826239671432000,
    "updated_at": 1695826239671432000
  }
}
```

## Working with leases

A process should create and maintain one or multiple leases while it is alive and use these leases
to hold semaphores. On startup the process calls `CreateSemaphoreLease` with some TTL, e.g. 60s.
Then periodically on a heartbeat it calls `RefreshSemaphoreLease` to extend TTL and signal that
it is still alive. In the meantime this lease can be used to call `AcquireSemaphore` and 
`ReleaseSemaphore`. On shutdown the process calls `RevokeSemaphoreLease` and everything that it
held will be released. If the process crashes everything will be released as well when TTL of its
lease is over.

Alternatively, a process can create a lease every time it needs to acquire a semaphore and set TTL
to the time it expects to work with that semaphore. Keep in mind that lease TTL starts counting right
after a lease is created, but an acquisition might not happen immediately because permits are not 
available.

## API reference

* [CreateSemaphore](/docs/api/v1beta/create-semaphore.md)
* [ListSemaphores](/docs/api/v1beta/list-semaphores.md)
* [GetSemaphore](/docs/api/v1beta/get-semaphore.md)
* [AcquireSemaphore](/docs/api/v1beta/acquire-semaphore.md)
* [ReleaseSemaphore](/docs/api/v1beta/release-semaphore.md)
* [UpdateSemaphore](/docs/api/v1beta/update-semaphore.md)
* [DeleteSemaphore](/docs/api/v1beta/delete-semaphore.md)
* [ListSemaphoreHolders](/docs/api/v1beta/list-semaphore-holders.md)
* [CreateSemaphoreLease](/docs/api/v1beta/create-semaphore-lease.md)
* [RevokeSemaphoreLease](/docs/api/v1beta/revoke-semaphore-lease.md)
* [RefreshSemaphoreLease](/docs/api/v1beta/refresh-semaphore-lease.md)
* [ListSemaphoreLeases](/docs/api/v1beta/list-semaphore-leases.md)
* [GetSemaphoreLease](/docs/api/v1beta/get-semaphore-lease.md)
