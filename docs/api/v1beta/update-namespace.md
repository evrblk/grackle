# UpdateNamespace

Updates a namespace's `description`. The namespace name itself cannot be changed.

Safe to retry.

## Request

```json
{
  "namespace_name": "UserObjects",
  "description": "Updated description"
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
    "updated_at": 1695826239925421000
  }
}
```
