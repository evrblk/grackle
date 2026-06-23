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
pkg/server/v1beta/server.go        GrackleApiServer  — implements gracklepb.GrackleApiServer
   │  Validate<Method>(req)  → InvalidArgument on failure; bumps prometheus op counters
   ▼
pkg/server/v1beta/handler.go       GrackleApiServerHandler
   │  - resolves NamespaceName → NamespaceId (GetNamespaceByName)  [almost every method does this first]
   │  - generates IDs (rand.Uint32/Uint64), stamps now = time.Now().UnixNano()
   │  - decodes/validates client lease IDs (ids.DecodeLeaseId), enforces ServiceLimits
   │  - converts front pb ⇄ core pb (pbconv.go), pagination tokens (base64 ⇄ core)
   │  - BLOCKING ops (AcquireLock/AcquireSemaphore/WaitForWaitGroup/WaitAtBarrier) poll in a loop
   │    with exponential backoff (100ms → 1s), honoring ctx + absolute deadline
   ▼
pkg/coreapis  GrackleClientApi (interface)        ← the seam between front-end and cores
   │  Two implementations (both generated):
   │   • GrackleMonsteraStub          — cluster mode: marshals → monsteraClient.Read/Update/UpdateShard(appName, shardKey, bytes)
   │   • GrackleNonclusteredStub      — single-node/tests: routes by shardKey to an in-process core slice (linear scan of bound ranges, RWMutex per core)
   ▼
pkg/<primitive>  Core               the actual state machine (locks, semaphores, waitgroups, barriers, namespaces)
   │  pure functions over a BadgerDB txn; no time/network of their own — `Now` is passed in every request
   ▼
pkg/tables  + monstera BinaryTable/indexes  →  BadgerDB
```

Key idea: **cores are deterministic state machines.** They take `Now` as input (never call `time.Now()`),
return errors as either Go `error` (infrastructure failure → bubbles up) or `ApplicationError`
(`monsterax.Error`, a domain error like NotFound/ResourceExhausted carried in the response payload).
This determinism is what lets Monstera replicate them via Raft.

## Package map (`pkg/`)

| Package | Role |
|---|---|
| `corepb/` | Core protobuf types + vtproto (marshal/unmarshal/size). `*.proto` → `*.pb.go` + `*_vtproto.pb.go`. Hand-written `sharding.go` (ShardKey per request), `marshal_gen.go` (generated `MarshalBinary`/`UnmarshalBinary` wrappers via genmarshal tool). |
| `coreapis/` | **Generated** by `monstera code generate` from `monstera.yaml`. `api.go` (typed request/response aliases + `Grackle*CoreApi` interfaces + `GrackleClientApi`), `adapters.go` (Monstera `ApplicationCore` adapters: method-number switch, metrics, marshal), `stubs.go` (`GrackleMonsteraStub` cluster client + `GrackleNonclusteredStub` in-process client + cores factory). DO NOT EDIT generated files. |
| `<primitive>/` (`locks`, `semaphores`, `waitgroups`, `barriers`, `namespaces`) | The cores. Each has `core.go` (the `Core` struct + `Grackle<X>CoreApi` impl) plus feature files (e.g. locks: `ancestors.go`, `locks.go`; semaphores: `holders.go`, `expiration.go`; waitgroups: `jobs.go`, `deletion.go`, `expiration.go`; barriers: `participants.go`, `expiration.go`). |
| `tables/` | Reusable BadgerDB table abstractions over monstera `BinaryTable`/indexes. `prefixes.go` = **the central registry of all 1-byte table prefixes** (`tables.Grackle["Grackle.LocksCore.Leases.Table"]`). `leases.go` (shared by locks+semaphores), generic `counters.go` (`CountersTable[T,U]`), `gc_records.go`. |
| `sharding/` | `ByAccount(accountId)` and `ByAccountAndNamespace(accountId, namespaceId)` → 4-byte truncated hash. This is the shard key. |
| `ids/` | Public string IDs ⇄ core pb IDs. base62-encoded, type-prefixed: `ns_`, `wg_`, `sem_`, `bar_`, `ls_` (lease). Layout: accountId(8) + namespaceId(4) [+ entityId(8)]. |
| `pagination/` | Pagination token helpers; core token ⇄ monstera token; `GetLimitWithDefaults`. |
| `grackle/` | `limits.go` — `ServiceLimits` struct + `DefaultServiceLimits` (per-namespace caps, rate limits). Passed into every handler call. |
| `server/v1beta/` | gRPC front-end. `server.go` (validate+dispatch), `handler.go` (orchestration), `validators.go` (`Validate*Request` + name/length/metadata regexes), `pbconv.go` (`*ToFront`/`*ToCore`), `middleware.go` (auth: request signing via `evrblk-go/authn`, hot-reloaded keys), `metrics.go`, `vtproto.go`. `integration_test/` = black-box tests against a full in-process server. |
| `workers/` | GC workers (`IntervalWorker`, every 5s). One per primitive. Each lists shards (`ListShards`) and fans out `Run<X>GarbageCollection` per shard concurrently. |

## cmd / run modes (`cmd/grackle`)

`grackle run <mode>` (cobra; `root.go`→`run.go`→subcommands):

- **single-node** (`single_node.go`): one shared `BadgerStore`, `GrackleNonclusteredStub` over an in-process cores factory (`--shards` internal shards, default 64), gRPC gateway + all 4 GC workers in one process. Simplest path; also the shape used by integration tests.
- **node** (`node.go`): a stateful Monstera node. Registers `ApplicationCoreDescriptors` (one per app: `GrackleLocks`, `GrackleSemaphores`, `GrackleWaitGroups`, `GrackleBarriers`, `GrackleNamespaces`) wrapping cores in generated adapters. Raft-replicated, sharded per `cluster_config.json`.
- **gateway** (`gateway.go`): stateless. `monstera.NewMonsteraClient(clusterConfig,...)` → `GrackleMonsteraStub` → gRPC server. No local state.
- **worker** (`worker.go`): stateless. Same Monstera client/stub, runs the 4 GC workers only.

Cluster apps and shard counts are declared in the Monstera cluster config (see README for the `monstera config add-application` invocations).

## Cores: conventions & invariants

- **Constructor**: `NewCore(badgerStore, shardGlobalIndexPrefix, shardLowerBound, shardUpperBound)`. Bounds delimit this shard's local key range (for Snapshot/Restore); `shardGlobalIndexPrefix` (a per-shard hash, `utils.GetTruncatedHash([]byte(shardId),4)`) scopes cross-shard *global* indexes (lease expiration index, GC records). Namespaces core has no global-index prefix arg.
- **`var _ coreapis.Grackle<X>CoreApi = &Core{}`** compile-time interface assertion at top of each `core.go`.
- **Standard methods**: `Snapshot()` / `Restore(reader)` (delegate to `monsterax.Snapshot/Restore` over `c.ranges()`), `Close()` (usually empty — Badger store is shared, not owned).
- **Transactions**: reads use `badgerStore.View()`, writes use `badgerStore.Update()`. Always `defer txn.Discard()`, then `txn.Commit()` at the end. A method either commits all its mutations or none.
- **Expiration is lazy + GC.** Reads (`GetLock`, `ListLocks`) compute "effective" state against `Now` by evicting expired lease holders on the fly (`checkLockExpiration` clones via `proto.Clone`). `GetLock`/mutating paths additionally *delete* drained rows and fix counters; `List*` paths merely *filter* (leave deletion to GC). The GC workers (`Run<X>GarbageCollection`) do the actual reaping, bounded per call (`MaxVisitedLocks`, page sizes) so a tick never runs unbounded.
- **Counters**: per-(account,namespace) `CountersTable` tracks `NumberOfLocks`, `NumberOfLeases`, etc. Enforced against `ServiceLimits` (e.g. `ResourceExhausted` when `NumberOfLocks > MaxNumberOfLocksPerNamespace`). Kept in sync on every create/delete — easy to break, watch it in tests.
- **Namespace deletion is async**: `DeleteNamespace` (handler) writes a GC marker into *each* primitive core (`<X>DeleteNamespace` → a `GarbageCollectionRecord`) then deletes the namespace row; the per-primitive GC workers later sweep all entities in that namespace.
- **Hierarchical locks** (`locks/core.go`, `ancestors.go`): lock name `a/b/c` has ancestors `["a","a/b"]` (`lockAncestorNames`). An `ancestorsTable` keeps per-prefix `ExclusiveCount`/`SharedCount` so acquisition can check descendant conflicts in O(1) without scanning. `incrementAncestors`/`decrementAncestors`/`swapAncestorMode` maintain it; `checkHierarchicalConflicts` = ancestor lookups + descendant counts.

## Tables / keyspace

- All table prefixes are 1 byte, registered in `tables/prefixes.go`. Ranges per primitive: Locks `0x01–0x09`, Semaphores `0x20–0x2a`, WaitGroups `0x40–0x46`, Namespaces `0x50–0x52`, Barriers `0x60–0x65`.
- Primary keys generally: `sharding.ByAccountAndNamespace(acct,ns) ++ accountId ++ namespaceId ++ <sortkey>`. The leading shard-key hash keeps a namespace's data co-located in one shard.
- Secondary indexes: monstera `OneToManyUint64Index` (e.g. lease-by-processId, locks-by-leaseId), `SortedIndex` (lease expiration — a **global** index keyed by `shardGlobalIndexPrefix ++ time ++ ...` so a shard can scan all of its leases by expiry regardless of namespace).
- `LeasesTable` is shared verbatim between locks and semaphores cores (same lease semantics).

## Code generation (`make generate`)

1. **protoc** over `pkg/corepb/*.proto` → `*.pb.go` (go) + `*_vtproto.pb.go` (marshal+unmarshal+size).
2. **`monstera code generate`** (config: `pkg/coreapis/monstera.yaml`) → regenerates `pkg/coreapis/{api,adapters,stubs}.go`. The yaml declares each core's `read_methods`/`update_methods` with a stable `method_number` and `sharded: true|false` (unsharded = `UpdateShard`/`ListShards`, used by GC). Adding a core method = edit yaml + regenerate + implement in the core.
3. **genmarshal** (`tools/codegen/genmarshal`, uses `dave/jennifer`) → `pkg/corepb/marshal_gen.go`: `encoding.BinaryMarshaler`/`Unmarshaler` impls so pb types satisfy the table generics (`tables.ptr[T]` constraint) and monstera's wire format.

Anything with `// Code generated by ... DO NOT EDIT.` — regenerate, don't hand-edit. That's `api.go`, `adapters.go`, `stubs.go`, all `*.pb.go`, `marshal_gen.go`.

## Testing conventions

- **`testify/require`** everywhere. Table-driven where it fits.
- **Two layers of tests**:
  - **Core unit tests** (`pkg/<primitive>/*_test.go`):
    - For core tests (`pkg/<primitive>/core_test.go`), construct a core over an in-memory Badger store. Helper `newLocksCore(t)` → `store.NewBadgerInMemoryStore()` + `NewCore(...)` with fixed bounds. Each test `init()` calls `tables.RegisterGracklePrefixes(...)`.
    - For tables tests do not use the core, use `store.NewBadgerInMemoryStore()` -> `new<X>Table()` directly.
  - **Integration tests** (`pkg/server/v1beta/integration_test/`): black-box against a real `GrackleApiServer` wired over `GrackleNonclusteredStub` (8 shards) + in-memory Badger. `setupGrackleApiServer(t)` (registers cleanup) vs `newGrackleApiServer(t)` (returns close func — used inside `synctest` bubbles so Badger goroutines drain before the bubble exits).
- **Structure**: top-level `TestCore_<Method>` / `Test<RpcMethod>`, with `t.Run("scenario", ...)` subtests. Common subtest names: `"exclusive"`, `"shared lock"`, `"validation"`, `"blocking"`, `"<state> by another process"`.
- **Test helpers** live at the *bottom* of the test file, all take `t` first and call `t.Helper()`. Convention pairs: `acquireLock`/`acquireLockWithError`, `createLease`/`createLeaseWithError`/`createLeaseWithMax`, `getLockLease`/`getLockLeaseWithError`. The `*WithError` variants assert `resp.Payload == nil && resp.ApplicationError != nil` and return the `*monsterax.Error`; the happy-path variants assert `ApplicationError == nil`.
- **Time**: tests pass explicit `now := time.Now()` and advance with `now.Add(...)`; expiry is tested by jumping `Now` past TTL. Blocking-RPC tests use Go 1.24 `testing/synctest` to fast-forward the polling sleeps.
- IDs/accounts in tests: `rand.Uint64()` / `rand.Uint32()` for account/namespace/lease ids.

## Conventions cheat-sheet

- Import order: stdlib → external (incl. other `evrblk/*`) → `evrblk/grackle/*`. `make format` = gofmt -s + goimports `-local github.com/evrblk/grackle`.
- Errors: infra failures → return Go `error` (caller bubbles up, becomes gRPC error via `monsterax.ErrorToGRPC`). Domain failures → `ApplicationError` (`monsterax.NewErrorWithContext(code, msg, ctxMap)`) in the response payload. `nilifyIfEmpty` turns empty errors into nil at the stub boundary.
- Validation lives only in `server/v1beta/validators.go` (front edge). Cores assume validated input but still enforce limits/existence.
- `Now` is always `time.Now().UnixNano()` (int64 nanos); TTLs are seconds in the API, converted to nanos in cores (`Now + ttl*1e9`).
- `make build` / `go test -v --race ./...` / `make lint` (fmt, vet, staticcheck, govulncheck) / `make grackle` (build binary) / `make grackle-image` (docker).

## Docs & tooling

- `docs/` is **product/user-facing** (overview, per-primitive guides, `docs/api/v1beta/<method>.md` per RPC). Keep that separate from these notes.
- `tools/dev/`: `compose-cluster` (docker-compose 3-node cluster + Prometheus/Grafana), `debug-cluster` (local multi-process), `load-generator`. Useful for manual cluster verification.
