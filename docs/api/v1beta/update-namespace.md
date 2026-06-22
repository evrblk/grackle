# UpdateNamespace

Updates a namespace's `description` and `metadata`. The namespace name itself cannot be changed.
`metadata` is an optional, opaque map of string key/value pairs; it is replaced wholesale, so an
update sent without it clears the existing metadata — see [Metadata](/docs/api-overview.md#metadata).

Safe to retry.

## Request

* `expected_version` enables optimistic locking: the update is applied only if it equals the
  namespace's current `version`. See [Updates](/docs/api-overview.md#updates).

```json
{
  "namespace_name": "UserObjects",
  "description": "Updated description",
  "expected_version": 1,
  "metadata": {
    "team": "identity"
  }
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Returns `InvalidArgument` if `expected_version` does not match the namespace's current `version`.
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
