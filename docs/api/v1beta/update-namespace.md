# UpdateNamespace

Updates a namespace's `description` and `metadata`. The namespace name itself cannot be changed.
`metadata` is an optional, opaque map of string key/value pairs; it is replaced wholesale, so an
update sent without it clears the existing metadata — see [Metadata](/docs/api-overview.md#metadata).

Safe to retry.

## Request

```json
{
  "namespace_name": "UserObjects",
  "description": "Updated description",
  "metadata": {
    "team": "identity"
  }
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* `version` is bumped on every successful update.

```json
{
  "namespace": {
    "name": "UserObjects",
    "description": "Updated description",
    "version": 2,
    "created_at": 1695826239671432000,
    "updated_at": 1695826239925421000,
    "metadata": {
      "team": "identity"
    }
  }
}
```
