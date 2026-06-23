# DeleteWaitGroup

Deletes a wait group immediately. Any in-flight `WaitForWaitGroup` callers will see `NotFound`. A
finished wait group (one that has `completed` or `expired`) is deleted automatically once its
`delete_after_finished_seconds` retention window elapses — explicit delete is only needed for early
cleanup.

Safe to retry.

## Request

```json
{
  "namespace_name": "pipelines",
  "wait_group_name": "batch_2026_06_12"
}
```

## Response

* Returns `NotFound` if the namespace does not exist.
* Deleting a wait group that does not exist is a no-op — no error.

```json
{}
```
