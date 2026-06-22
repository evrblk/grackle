# API Overview

Conventions that apply across the whole Everblack Grackle API. 

## Timestamps

Every timestamp in Grackle — both in requests and in responses — is an `int64` count of
**nanoseconds since the Unix epoch** (UTC). This includes fields such as `created_at`,
`updated_at`, `locked_at`, `arrived_at`, `completed_at`, and the absolute `expires_at` deadlines on
wait groups and barriers.

Durations are the exception: lease TTLs and blocking-call timeouts are expressed in **seconds**
(`ttl_seconds`, `timeout_seconds`), because they are relative offsets rather than points in time.

## Updates replace the whole record

`Update*` calls (`UpdateNamespace`, `UpdateWaitGroup`, `UpdateSemaphore`, `UpdateBarrier`) perform a
**full replacement** of the entity's mutable fields with the values in the request — they are not
partial patches and do not merge deltas. A field left at its zero value in the request is written
as that zero value: omitting `metadata` clears it, sending an empty `description` blanks it.

The practical rule: read-modify-write. Send the complete desired state of every mutable field, not
just the ones you intend to change. Immutable fields (a namespace's name, a wait group's `counter`,
etc.) are not part of the update surface — see each primitive's page for what is mutable.

## Metadata

Most Grackle entities carry an optional `metadata` map of string keys to string values. It is
opaque to Grackle — it is stored verbatim and returned on reads, never interpreted — and is meant
for attaching application context such as an owning team, a trace ID, or a deploy revision.

Metadata can be attached to:

* **Namespaces, wait groups, semaphores, and barriers** — set on the create call and replaced via
  the corresponding update call (`UpdateNamespace`, `UpdateWaitGroup`, `UpdateSemaphore`,
  `UpdateBarrier`).
* **Lock holders and semaphore holders** — set on `AcquireLock` / `AcquireSemaphore` and returned
  with the holder.
* **Barrier participants** — set on `ArriveAtBarrier` and returned by `ListBarrierParticipants`.
* **Completed wait-group jobs** — set per job on `CompleteJobsFromWaitGroup` and returned by
  `ListWaitGroupCompletedJobs`.

Metadata is bounded: at most 32 entries per map, keys up to 128 bytes, and values up to 256 bytes.
Because updates replace the whole record (see above), an update sent with no metadata clears it.

## Pagination

Every `List*` endpoint returns results one page at a time.

**Request.** Two fields control paging:

* `limit` — the maximum number of entries to return in the page. Valid range is 1–100; if it is 0
  or omitted, the default page size of 100 is used.
* `pagination_token` — an opaque cursor identifying where the page starts. Leave it empty to fetch
  the first page.

**Response.** Each page comes back with two cursors:

* `next_pagination_token` — pass it back as the request's `pagination_token` to fetch the following
  page. It is empty when the current page is the last one.
* `previous_pagination_token` — pass it back to fetch the preceding page. It is empty on the first
  page.

A pagination token is a position cursor, not a snapshot: walking the pages reflects entries that
were inserted or removed concurrently. List endpoints are read-only and safe to retry, so a page
can be re-fetched with the same token at any time.
