# CreateNamespace

Creates a new namespace. Namespace names are unique per account. Locks, semaphores, wait groups,
and barriers all live inside a namespace and are scoped to it.

Safe to retry — duplicate calls fail with `AlreadyExists` rather than creating two namespaces.

## Request

* `metadata` is an optional, opaque map of string key/value pairs stored alongside the namespace —
  see [Metadata](/docs/api-overview.md#metadata).

```json
{
  "name": "UserObjects",
  "description": "Per-user objects",
  "metadata": {
    "team": "search"
  }
}
```

## Response

* Returns `AlreadyExists` if a namespace with the same name exists.

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
