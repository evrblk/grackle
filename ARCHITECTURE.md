# Grackle Architecture Notes

> Internal reference for working in this repo. Optimized for fast re-orientation, not external docs.

## What it is

Distributed synchronization-primitives-as-a-service, built on the **Monstera** framework
(`github.com/evrblk/monstera` — sharded, Raft-replicated state machines over embedded BadgerDB).
Four primitives + namespaces:

- **Locks** — hierarchical (`a/b/c`), shared or exclusive, lease-held.
- **Semaphores** — weighted, permits-based, lease-held.
- **Wait groups** — fan-in of millions of jobs (like `sync.WaitGroup`), absolute-deadline expiry.
- **Barriers** — generational rendezvous for N processes, absolute-deadline expiry.

All state durable in BadgerDB. Lock/semaphore holds are **lease-based with TTL** (holder heartbeats);
wait groups/barriers have their own absolute deadlines. Crashes never leave dangling holds. Every op
is atomic and retry-safe. No external deps (no DB/Kafka/Redis/ZK).

## Layered request flow

```
gRPC client (evrblk-go gracklepb)
   │
   ▼
pkg/server/v1beta/middleware.go   AuthenticationMiddleware (optional) + MonitoringMiddleware
   │  - auth: verifies evrblk-signature/evrblk-api-key-id/evrblk-timestamp via evrblk-go/authn
   │    (key_alfa_/key_bravo_ schemes); keys are read from disk on first use per key id and
   │    cached in-process for the life of the process — NOT hot-reloaded/watched
   │  - monitoring: bumps prometheus request/failure counters + duration histogram
   ▼
pkg/server/v1beta/server.go        GrackleApiServer  — implements gracklepb.GrackleApiServer
   │  Validate<Method>(req)  → InvalidArgument on failure, then calls handler with
   │  hardcoded accountId=0 and grackle.DefaultServiceLimits (see single-tenant note below)
   ▼
pkg/server/v1beta/handler.go       GrackleApiServerHandler
   │  - resolves NamespaceName → NamespaceId first (almost every method), through an in-process
   │    TTL cache (positive hit TTL 5s, negative TTL 1s, swept every 5m) — not a raw lookup per call
   │  - generates IDs via rand.Uint64() (math/rand/v2), retried up to 5x on IDCollision
   │  - now := time.Now() (time.Time); converted to UnixNano only where compared/stored
   │  - decodes/validates client lease IDs (ids.DecodeLeaseId) + account/namespace ownership check
   │  - enforces ServiceLimits inline for a subset of ops, passes the rest through to the core
   │  - converts front pb ⇄ core pb (pbconv.go, mostly core→front plus a few nested front→core
   │    helpers), pagination tokens (base64 ⇄ core ⇄ honey.PaginationToken)
   │  - BLOCKING ops (AcquireLock/AcquireSemaphore/WaitForWaitGroup/WaitAtBarrier) poll in a loop
   │    with exponential backoff (100ms → 1s, doubling), honoring ctx + absolute deadline
   │  - errors bubble through mrpc.ErrorToGRPC(err)
   ▼
pkg/coreapis  GrackleClientApi (interface)        ← the seam between front-end and cores
   │  Two implementations (both generated):
   │   • GrackleMonsteraStub          — cluster mode: marshals → monsteraClient.Read/Update/UpdateShard(appName, shardKey, bytes)
   │   • GrackleNonclusteredStub      — single-node/tests: routes by shardKey to an in-process core slice (linear scan of bound ranges, RWMutex per core)
   ▼
pkg/<primitive>  Core               the actual state machine (locks, semaphores, waitgroups, barriers, namespaces)
   │  pure functions over a BadgerDB txn; no time/network of their own — `Now` is passed in every request
   ▼
pkg/tables  + honey.BinaryTable/indexes  →  BadgerDB (rows exclusively owned by this core — see "Tables / keyspace")
```

Key idea: **cores are deterministic state machines.** They take `Now` as input (never call `time.Now()`),
return errors as either Go `error` (infrastructure failure → bubbles up, becomes a gRPC error via
`mrpc.ErrorToGRPC`) or an `ApplicationError` (`*mrpc.Error` from `github.com/evrblk/monstera/rpc`,
a domain error like NotFound/ResourceExhausted/IDCollision carried in the response payload).
This determinism is what lets Monstera replicate them via Raft. There is no `monsterax` package
anymore — its role was absorbed by `github.com/evrblk/monstera/rpc` (imported as `mrpc`) for
errors/request-response types, and by `github.com/evrblk/yellowstone-common/honey` (see below)
for tables, indexes, and portable snapshot/restore.

## Package map (`pkg/`)

| Package | Role |
|---|---|
| `corepb/` | Core protobuf types + vtproto (marshal/unmarshal/size). `*.proto` → `*.pb.go` + `*_vtproto.pb.go`. Hand-written `sharding.go` (`ShardKey()` per request type — a **routing** key only, see "Tables / keyspace"), `gc_identity.go` (`Identity()` on each GC-record oneof, used by `tables.GCRecordsTable.RestoreEntity` to bounds-filter a streamed GC record during split/restore), `marshal_gen.go` (generated `MarshalBinary`/`UnmarshalBinary` wrappers via the external genmarshal tool). |
| `coreapis/` | **Generated** by `monstera code generate` from `monstera.yaml`. `api.go` (typed request/response aliases over `mrpc.ReadRequest[T]`/`mrpc.UpdateRequest[T]`/`mrpc.UpdateUnshardedRequest[T]` generics + `Grackle*CoreApi` interfaces + `GrackleClientApi`), `adapters.go` (Monstera `ApplicationCore` adapters: method-number switch, metrics, marshal), `stubs.go` (`GrackleMonsteraStub` cluster client + `GrackleNonclusteredStub` in-process client + cores factory). DO NOT EDIT generated files. |
| `<primitive>/` (`locks`, `semaphores`, `waitgroups`, `barriers`, `namespaces`) | The cores. Each has `core.go` (the `Core` struct + `Grackle<X>CoreApi` impl + `table_prefixes.go` for its own 1-byte table prefixes) plus feature files (e.g. locks: `ancestors.go`, `locks.go`; semaphores: `holders.go`, `expiration.go`; waitgroups: `jobs.go`, `deletion.go`, `expiration.go`; barriers: `participants.go`, `expiration.go`). |
| `tables/` | Reusable BadgerDB table abstractions built on `honey.BinaryTable`/indexes: `leases.go` (shared by locks+semaphores), generic `counters.go` (`CountersTable[T,U]`), `gc_records.go`. No central prefix registry anymore — see "Tables / keyspace". |
| `sharding/` | `ByAccount(accountId)` and `ByAccountAndNamespace(accountId, namespaceId)` → 4-byte truncated hash (`cluster.ShardKey`). Used only for (1) request routing — `corepb`'s `ShardKey()` methods feed this into the stub/Monstera client — and (2) the `bounds.Owns(...)` ownership predicate during portable restore/split. It no longer determines where a row physically lives on disk. |
| `ids/` | Public string IDs ⇄ core pb IDs, base62-encoded, type-prefixed: `ns_`, `wg_`, `sem_`, `bar_`, `ls_` (lease). Pluggable via an `Encoder` interface with two implementations: `SingleTenantIDsEncoder` (the package default — drops `accountId` from the wire format entirely, always `0` on decode; layout `namespaceId(8) [+ entityId(8)]`) and `MultiTenantIDsEncoder` (`accountId(8) + namespaceId(8) [+ entityId(8)]`, for the cloud build). |
| `pagination/` | Pagination token helpers; core token ⇄ `honey.PaginationToken` ⇄ base64; `GetLimitWithDefaults`. |
| `grackle/` | `limits.go` — `ServiceLimits` struct + `DefaultServiceLimits` (per-namespace caps, rate limits). Passed into every handler call. |
| `server/v1beta/` | gRPC front-end. `server.go` (validate+dispatch), `handler.go` (orchestration, namespace cache), `validators.go` (`Validate*Request` + name/length/metadata regexes — the *only* place structural request validation lives), `pbconv.go` (`*ToFront`/`*ToCore`), `middleware.go` (`AuthenticationMiddleware` — signing via `evrblk-go/authn`, keys cached for process lifetime, not hot-reloaded — and `MonitoringMiddleware` for prometheus counters/latency), `metrics.go` (metric definitions + `RegisterMetrics()`), `vtproto.go` (registers a process-global `"proto"` gRPC codec that prefers vtproto Marshal/Unmarshal, falling back to standard proto for messages without vtproto helpers, e.g. health/reflection). `integration_test/` = black-box tests calling a full in-process `GrackleApiServer` directly. |
| `workers/` | GC workers (`IntervalWorker`, every 5s). One per primitive (no Namespaces GC worker — namespace deletion of the namespace row itself is synchronous, see below). Each lists shards (`ListShards("Grackle<X>")`) and fans out `Run<X>GarbageCollection` per shard concurrently. Dispatch is per **shard**, unrelated to the per-replica key-prefix scheme below. |

## Single-tenant (OSS) vs multi-tenant (cloud)

**This is the open-source, single-tenant build.** That is why `GrackleApiServer` (`server.go`)
passes a hardcoded `accountId = 0` and `grackle.DefaultServiceLimits` (high, fixed per-namespace
caps) into every handler call, and why `ids.DefaultEncoder` is a `SingleTenantIDsEncoder` that
omits `accountId` from public IDs entirely. The per-account plumbing throughout the rest of the
stack — `AccountId` in every core ID and shard key, per-`(account,namespace)` counters, the
namespace/lease ownership checks, `handler.go`'s `accountId`/`limits` parameters on every method —
is **real and intentional**, not dead code: this is inferred from the handler's signature design
(there's no comment spelling out the split), and the closed-source cloud build is expected to reuse
the same `handler.go`/cores/tables while swapping in its own front server (deriving `accountId` from
the authn key, using `MultiTenantIDsEncoder`, and fetching `ServiceLimits` per account) plus a
different `ids.Encoder`. `GrackleApiServer` itself is **not** reused by cloud.

Implication for reviewers: do not flag the hardcoded `accountId = 0`, the hardcoded limits, or the
"single-tenant collapse" as bugs — they are the designed OSS behavior. Per-account isolation
correctness is exercised by the cloud front-end, not this one.

## cmd / run modes (`cmd/grackle`)

`grackle run <mode>` (cobra; `root.go`→`run.go`→subcommands). `discovery.go` holds a shared
`--monstera-nodes`/`--monstera-nodes-file`/`--monstera-nodes-srv` flag set (exactly one required)
used by `gateway` and `worker` to build a `monstera.NodeDiscovery`.

- **single-node** (`single_node.go`): one shared `BadgerStore`. A `honey.NewReplicaPrefixRegistry(dataStore)` hands out a stable 2-byte prefix per internal shard (see "Cores: conventions & invariants" — this replaces the old truncated-hash shard prefix). `GrackleNonclusteredStub` over an in-process cores factory (`--shards` internal shards, default 64), gRPC gateway + the 4 GC workers in one process; `--auth-keys-path` optionally wires the auth interceptor. Simplest path; also the shape used by integration tests (with a third, test-only prefix scheme — see "Testing conventions").
- **node** (`node.go`): a stateful Monstera node. Registers `ApplicationCoreDescriptors` (one per app: `GrackleLocks`, `GrackleSemaphores`, `GrackleWaitGroups`, `GrackleBarriers`, `GrackleNamespaces`), all wired as `monstera.CoreTypePersistedExclusive`, wrapping cores in generated adapters. A `honey.ReplicaPrefixRegistry` (keyed by `replica.Id` this time, not shard id) supplies each core's `replicaPrefix`. Raft-replicated. Flags are only `--data-dir`, `--listen`, `--prometheus-port` — **no static cluster-config file and no `--node-id`**; node identity/topology comes from Monstera's own cluster/admin machinery, not a file passed on the CLI.
- **gateway** (`gateway.go`): stateless. Builds a `monstera.NodeDiscovery` from the shared discovery flags, then `monstera.NewPollingClusterConfigProvider(discovery, adminClient, ...)` — the gateway learns cluster topology from the cluster itself and keeps polling/refreshing it, rather than reading a static config. `monstera.NewMonsteraClient(provider, transport, ...)` → `GrackleMonsteraStub` → gRPC server. No local state.
- **worker** (`worker.go`): stateless. Same discovery/provider/client wiring as gateway, runs the 4 GC workers only, no gRPC server.

There is **no `cluster_config.json` in production run modes anymore** — that static-file model
(and `monstera config add-application`) only survives in the `tools/dev/debug-cluster` and
`tools/dev/compose-cluster` local dev harnesses, which are separate from how `node`/`gateway`/`worker`
actually discover the cluster. (`docs/getting-started.md` still documents the old
`--monstera-config`/`--node-id` flags — that page is stale too and due its own pass.)

## Cores: conventions & invariants

- **Constructor**: `NewCore(badgerStore *store.BadgerStore, replicaPrefix []byte, shardLowerBound, shardUpperBound cluster.ShardKey)`. `replicaPrefix` is a **replica**-unique, node-local two-byte prefix assigned by `honey.ReplicaPrefixRegistry` (`github.com/evrblk/yellowstone-common/honey`) and nested ahead of every table's own 1-byte prefix, so every row is exclusively owned by this one core (`monstera.CoreTypePersistedExclusive` — no aliasing between cores/replicas sharing a BadgerStore). Record keys carry no shard-key material; routing mismatches are rejected upstream by the generated adapter's shard-bounds check. The lower/upper bounds still delimit the shard's key range and drive the bounds-filtered portable `Restore`. This replaced the older model where a `shardGlobalIndexPrefix` (a hash of the shard id) partitioned a *shared* keyspace by shard-key range (`monstera.CoreTypePersistedShared`) — the new exclusive-prefix model is what makes live shard splits possible: a splitting parent's snapshot streams straight into a still-serving, independently-prefixed child core with no key collision.
- **`var _ coreapis.Grackle<X>CoreApi = &Core{}`** compile-time interface assertion at top of each `core.go`.
- **Standard methods**: each core defines `snapshotSections() []honey.Section{{Name, Table}, ...}` listing its own tables; `Snapshot()` = `honey.NewSnapshot(badgerStore, "Grackle<X>", sections)`; `Restore(readers...)` = `honey.RestoreSnapshot(badgerStore, sections, honey.ShardRange{Lower, Upper}, readers...)`. `Close()` is an empty no-op (the Badger store is shared, not owned).
- **Transactions**: reads use `badgerStore.View()`, writes use `badgerStore.Update()`. Always `defer txn.Discard()`, then `txn.Commit()` at the end. A method either commits all its mutations or none.
- **Expiration is lazy + GC**, unaffected by the replica-prefix change. Reads (`GetLock`, `ListLocks`) compute "effective" state against `Now` by evicting expired lease holders on the fly (`checkLockExpiration`, read-only). `GetLock`/mutating paths additionally *delete* drained rows and fix counters; `List*` paths merely *filter* (leave deletion to GC). The GC workers (`Run<X>GarbageCollection`) do the actual reaping, bounded per call (`MaxVisitedLocks`/`MaxVisited`, page sizes) so a tick never runs unbounded.
- **Counters**: per-(account,namespace) `CountersTable` tracks `NumberOfLocks`, `NumberOfLeases`, etc. — still keyed by `(accountId, namespaceId)` *inside* the replica-owned table, not per-replica. Enforced against `ServiceLimits` (e.g. `ResourceExhausted` when `NumberOfLocks > MaxNumberOfLocksPerNamespace`). Kept in sync on every create/delete — easy to break, watch it in tests.
- **Namespace deletion is async for the primitives, synchronous for the namespace row itself**: `namespaces.Core.DeleteNamespace` only removes the namespace row + its own counter; the primitives living in that namespace are torn down separately by the front handler's cross-primitive fan-out, which calls each primitive's `<X>DeleteNamespace` (`handler.go` → `LocksDeleteNamespace`/etc.) writing a GC marker into that core (`GarbageCollectionRecord`). The per-primitive GC workers later sweep all entities in that namespace.
- **Hierarchical locks** (`locks/core.go`, `ancestors.go`): lock name `a/b/c` has ancestors `["a","a/b"]` (`lockAncestorNames`). An `ancestorsTable` (constructed with `replicaPrefix`, same as every other table) keeps per-prefix `ExclusiveCount`/`SharedCount` so acquisition can check descendant conflicts in O(1) without scanning. `incrementAncestors`/`decrementAncestors`/`swapAncestorMode` maintain it; `checkHierarchicalConflicts` = ancestor lookups + descendant counts.

## Tables / keyspace

- **No central prefix registry anymore.** There is no `tables/prefixes.go`; each primitive package
  owns its own `table_prefixes.go` (e.g. `pkg/locks/table_prefixes.go`, and one each for
  `semaphores`/`waitgroups`/`namespaces`/`barriers`), with 1-byte table prefixes that only need to
  be unique *within that core* — the same byte value is reused across different primitives on
  purpose, since each core's tables live under its own replica prefix.
- **Physical key layout**: `[replicaPrefix (2 bytes, per Raft replica, from honey.ReplicaPrefixRegistry)] ++ [table prefix (1 byte)] ++ [record key]`. Record keys generally: `accountId ++ namespaceId ++ <sortkey>` — no shard-key hash is baked into the stored key anymore (contrast with `sharding.ByAccount(AndNamespace)`, which is purely a routing/ownership-check value, not a storage prefix).
- Secondary indexes (`honey.OneToManyUint64Index`, `honey.OneToManySortedIndex`, `honey.SortedIndex` — e.g. lease-by-processId, locks-by-leaseId, lease expiration) are, like primary tables, exclusive to the owning core/replica now — not cross-shard "global" indexes. The lease expiration index, for instance, is scoped as `replicaPrefix ++ tablePrefix(expirationIndex) ++ time ++ ...` within this core only.
- `LeasesTable` is still shared verbatim between locks and semaphores cores (same lease semantics).
- All of this sits on generic primitives from `github.com/evrblk/yellowstone-common/honey`: `honey.BinaryTable[T,U]` (primary/secondary tables), `honey.Uint64Table` (name→id indexes), and the `honey.Section`/`honey.ShardRange`/`NewSnapshot`/`RestoreSnapshot` machinery backing every core's `Snapshot`/`Restore`. Each table's `RestoreEntity(txn, key, value, bounds honey.ShardRange)` filters via `bounds.Owns(sharding.ByAccountAndNamespace(...))` to decide whether a streamed row (during a split/merge) belongs to this shard.

## Code generation (`make generate`)

1. **protoc** over `pkg/corepb/*.proto` → `*.pb.go` (go) + `*_vtproto.pb.go` (marshal+unmarshal+size).
2. **`go tool github.com/evrblk/monstera/cmd/monstera code generate`** (config: `pkg/coreapis/monstera.yaml`) → regenerates `pkg/coreapis/{api,adapters,stubs}.go`. The yaml declares each core's `read_methods`/`update_methods` with a stable `method_number` and `sharded: true|false` (unsharded = `UpdateShard`/`ListShards`, used by GC). Adding a core method = edit yaml + regenerate + implement in the core.
3. **`go tool github.com/evrblk/yellowstone-common/codegen/genmarshal -dir ./pkg/corepb -output ./pkg/corepb/marshal_gen.go`** → `encoding.BinaryMarshaler`/`Unmarshaler` impls so pb types satisfy the table generics and monstera's wire format. This tool now lives in the external `yellowstone-common` module (invoked via Go's `tool` directive), not in this repo's `tools/`.

Anything with `// Code generated by ... DO NOT EDIT.` — regenerate, don't hand-edit. That's `api.go`, `adapters.go`, `stubs.go`, all `*.pb.go`, `marshal_gen.go`.

## Testing conventions

- **`testify/require`** everywhere. Table-driven where it fits.
- **Two layers of tests**, and three distinct ways a `replicaPrefix`/table prefix gets supplied depending on layer:
  - **Core unit tests** (`pkg/<primitive>/*_test.go`):
    - `newLocksCore(t)` (and its per-primitive equivalents) construct a core over an in-memory Badger store with a **hardcoded literal** replica prefix and fixed bounds, e.g. `NewCore(badgerStore, []byte{0x1d, 0x36, 0x00, 0x00}, 0x00000000, 0xffffffff)` — there is no registry lookup and no `init()`/`RegisterGracklePrefixes` call in tests.
    - For tables tests do not use the core; construct `new<X>Table(...)` (or `NewLeasesTable`/`NewCountersTable`/etc.) directly with a literal prefix and operate on `store.NewBadgerInMemoryStore()` transactions.
  - **Integration tests** (`pkg/server/v1beta/integration_test/`): black-box against a real `GrackleApiServer` wired over `GrackleNonclusteredStub` (**8 shards**) + in-memory Badger; here each core's prefix comes from `utils.GetTruncatedHash([]byte(shardId), 4)` — a third scheme, distinct from both production (`honey.ReplicaPrefixRegistry`) and core unit tests (hardcoded literal). `setupGrackleApiServer(t)` (registers `t.Cleanup`) vs `newGrackleApiServer(t)` (returns a close func — used inside `synctest` bubbles so Badger goroutines drain before the bubble exits).
- **Structure**: top-level `TestCore_<Method>` / `Test<RpcMethod>`, with `t.Run("scenario", ...)` subtests. Common subtest names: `"exclusive"`, `"shared lock"`, `"validation"`, `"blocking"`, `"<state> by another process"`.
- **Test helpers** live at the *bottom* of the test file, all take `t` first and call `t.Helper()`. Convention pairs: `acquireLock`/`acquireLockWithError`, `createLease`/`createLeaseWithError`/`createLeaseWithMax`, `getLockLease`/`getLockLeaseWithError`. The `*WithError` variants assert `resp.Payload == nil && resp.ApplicationError != nil` and return the `*mrpc.Error` (from `github.com/evrblk/monstera/rpc`); the happy-path variants assert `ApplicationError == nil`.
- **Time**: tests pass explicit `now := time.Now()` and advance with `now.Add(...)`; expiry is tested by jumping `Now` past TTL. `testing/synctest` is used for blocking-RPC tests, but **only** for `AcquireLock`/`AcquireSemaphore` (`locks_test.go`, `semaphores_test.go`) — `WaitAtBarrier`/`WaitForWaitGroup` blocking tests still use plain `time.Sleep`, not a synctest bubble; worth harmonizing if you touch that area.
- IDs/accounts in tests: `rand.Uint64()` / `rand.Uint32()` for account/namespace/lease ids.

## Conventions cheat-sheet

- Import order: stdlib → external (incl. other `evrblk/*`) → `evrblk/grackle/*`. `make format` = gofmt -s + goimports `-local github.com/evrblk/grackle`.
- Errors: infra failures → return Go `error` (caller bubbles up, becomes gRPC error via `mrpc.ErrorToGRPC`). Domain failures → `*mrpc.Error` (`mrpc.NewErrorWithContext(code, msg, ctxMap)`, `github.com/evrblk/monstera/rpc`) in the response payload.
- Validation lives only in `server/v1beta/validators.go` (front edge). Cores assume validated input but still enforce limits/existence.
- `Now` is stamped once per request as `time.Time` in `handler.go`, converted to `int64` nanoseconds only where compared against or stored in a proto field; TTLs are seconds in the API, converted to nanos in cores (`Now + ttl*1e9`).
- `make build` / CI runs `go test -v --race ./...` (no `make test` target) / `make lint` (fmt, vet, staticcheck, govulncheck) / `make grackle` (build binary) / `make grackle-image` (docker).

## Docs & tooling

- `docs/` is **product/user-facing** (`nav.yaml` drives `overview.md`, `getting-started.md`, `api-overview.md`, per-primitive guides `locks.md`/`semaphores.md`/`wait-groups.md`/`barriers.md`, and `docs/api/v1beta/<method>.md` per RPC). Keep that separate from these notes. Note: `getting-started.md`'s clustered-mode walkthrough (`--monstera-config`, `--node-id`, `monstera config add-application`) is currently stale against the flags described above under "cmd / run modes" and needs its own update pass.
- `tools/dev/`: `compose-cluster` (docker-compose multi-node cluster + Prometheus/Grafana, `monstera cluster add-node`/`move-shard` for manual shard-movement testing), `debug-cluster` (single-process multi-node dev cluster, `grpc`/`local` transport, still driven by a static `cluster_config.json` generated locally — this is a dev-only artifact, not how production `node`/`gateway`/`worker` discover the cluster), `load-generator` (load-tests all four primitives, bounded in-flight blocking-call pool, Prometheus metrics). Useful for manual cluster verification.
