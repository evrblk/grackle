# GetSemaphoreLease

Fetches a semaphore lease by id.

Read-only and safe to retry.

## Request

```json
{
  "namespace_name": "third_parties",
  "lease_id": "ls_NfKKeiPbP18NFeU3lLGrRWWgDJRB"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `NotFound` if the lease does not exist or has already expired.

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
