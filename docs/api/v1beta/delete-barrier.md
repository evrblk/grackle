# DeleteBarrier

Deletes a barrier. Any in-flight `WaitAtBarrier` callers will see `NotFound`.

A barrier is deleted automatically once it has been inactive for `delete_inactive_after_seconds`
(measured from its last activity) — explicit delete is only needed for early cleanup.

Safe to retry.

## Request

```json
{
  "namespace_name": "pipelines",
  "barrier_name": "phase_1_complete"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Deleting a barrier that does not exist is a no-op — no error.

```json
{}
```
