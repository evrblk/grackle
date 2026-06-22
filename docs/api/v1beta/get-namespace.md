# GetNamespace

Fetches a namespace by name.

Read-only and safe to retry.

## Request

```json
{
  "namespace_name": "UserObjects"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* `metadata` is the optional, opaque map stored with the namespace — see [Metadata](/docs/api-overview.md#metadata).

```json
{
  "namespace": {
    "name": "UserObjects",
    "description": "Per-user objects",
    "version": 1,
    "created_at": 1695826239671432000,
    "updated_at": 1695826239671432000,
    "metadata": {
      "team": "search"
    }
  }
}
```
