# GetLockLease

Fetches a lock lease by id.

Read-only and safe to retry.

## Request

```json
{
  "namespace_name": "UserObjects",
  "lease_id": "ll_NfKKeiPbP18NFeU3lLGrRWWgDJRB"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the lease does not exist or has already expired.
* `now` is the server clock (Unix nanoseconds) at the moment this response was produced — use
  it, not your local clock, to compute remaining time against `expires_at`.

```json
{
  "lease": {
    "lease_id": "ll_NfKKeiPbP18NFeU3lLGrRWWgDJRB",
    "process_id": "host-123/pid-4567",
    "created_at": 1695826239671432000,
    "expires_at": 1695826299671432000
  },
  "now": 1695826270671432000
}
```
