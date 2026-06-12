# ListBarrierParticipants

Lists participants that have arrived at a specific generation of a barrier. Paginated. Useful for
diagnostics — "which peers have not yet shown up for this round?"

Read-only and safe to retry.

## Request

* `generation` must be specified — pass the current generation from `GetBarrier`, or a past one
  for forensic inspection.
* Leave `pagination_token` empty for the first page.
* `limit` sets the number of entries per page.


```json
{
  "namespace_name": "pipelines",
  "barrier_name": "phase_1_complete",
  "generation": 1,
  "pagination_token": "",
  "limit": 100
}
```

## Response

* Returns `NotFound` if the barrier does not exist.
* Returns `NotFound` if the namespace does not exist.
* Non-empty `next_pagination_token` indicates more pages are available.

```json
{
  "participants": [
    {
      "process_id": "shard-0",
      "arrived_at": 1718150410000000000
    },
    {
      "process_id": "shard-1",
      "arrived_at": 1718150412000000000
    }
  ],
  "next_pagination_token": "",
  "previous_pagination_token": ""
}
```
