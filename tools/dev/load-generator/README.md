# Grackle Load Generator

A flexible, high-performance load testing tool for Grackle.

## Overview

The load generator spawns N worker goroutines that generate concurrent requests to a Grackle server, exercising:
- **Locks** (shared and exclusive): acquire (blocking), release, and read state
- **Semaphores**: acquire (blocking), release, and read state
- **Wait Groups**: complete jobs, raise the counter, wait (blocking), and read state
- **Barriers**: arrive, wait (blocking), update, and read state

The tool provides:
- Configurable workload patterns (operation mix, read/write ratio)
- Real-time statistics and Prometheus metrics
- Graceful shutdown and optional resource cleanup
- Rate limiting for controlled load

### Blocking calls

`AcquireLock`, `AcquireSemaphore`, `WaitForWaitGroup`, and `WaitAtBarrier` all block server-side
for up to a configurable timeout. To keep these from stalling load generation, the generator runs
them on a bounded pool of background goroutines (`--max-inflight-blocking`): a worker fires the
blocking call and immediately moves on to the next operation. When the pool is saturated the
blocking op is dropped (and counted in `load_generator_blocking_dropped_total`) rather than
blocking the worker.

### Resource lifecycle

The generator keeps each primitive's lifecycle coherent with the API:
- **Wait groups** are completed using a fixed `[0, counter)` job-ID space, so a group is never
  completed beyond its counter, and completions are idempotent. The counter is only ever raised,
  and only while the group is `active` — a finished group is never modified. When a group
  completes it is recreated so load keeps flowing.
- **Barriers** simulate `--barrier-expected-processes` logical participants per generation so they
  actually trip and advance generations. Arrivals and waits carry the generation the generator
  currently tracks, reconciled from every response.

## Prerequisites

A running Grackle cluster with gateway enabled. You can use:
- `tools/dev/debug-cluster` for local testing
- `tools/dev/compose-cluster` for Docker-based testing
- Any Grackle deployment

## Quick Start

### 1. Start a Grackle cluster with gateway

```bash
cd tools/dev/debug-cluster
./generate_config.sh
go run . --gateway-port=9000
```

### 2. Run the load generator

```bash
cd tools/dev/load-generator
go run . --endpoint=localhost:9000 --workers=50 --duration=30s
```

You should see output like:
```
[00:00:15] Requests: 239,629 | Errors: 671 | RPS: 15,975 | Success: 99.7%
  Locks: 71,681 (30%) | Semaphores: 59,894 (25%) | WaitGroups: 59,873 (25%) | Barriers: 48,181 (20%)
```

### 3. View metrics

While the load generator is running, visit:
- Load generator metrics: http://localhost:2113/metrics
- Cluster metrics: http://localhost:2112/metrics

## Configuration

### Connection

- `--endpoint` - Grackle gateway endpoint (default: `localhost:9000`)

### Load Parameters

- `--workers` - Number of concurrent workers (default: `10`)
- `--duration` - Load test duration (default: `60s`, `0` = infinite)
- `--rate` - Target operations per second (default: `0` = unlimited)
- `--max-inflight-blocking` - Max concurrent blocking calls (acquire/wait) across all workers (default: `1000`)

### Resources

- `--namespaces` - Number of namespaces to create (default: `5`)
- `--locks-per-ns` - Locks per namespace (default: `100`)
- `--semaphores-per-ns` - Semaphores per namespace (default: `20`)
- `--waitgroups-per-ns` - Wait groups per namespace (default: `10`)
- `--barriers-per-ns` - Barriers per namespace (default: `10`)

### Operation Mix

Percentages must sum to 100:
- `--locks-pct` - Percentage of lock operations (default: `30`)
- `--semaphores-pct` - Percentage of semaphore operations (default: `25`)
- `--waitgroups-pct` - Percentage of wait group operations (default: `25`)
- `--barriers-pct` - Percentage of barrier operations (default: `20`)

### Read/Write Ratio

- `--read-pct` - Percentage of read operations for each type (default: `30`)

Applies to all primitive types. Reads are `Get*`/`List*` plus the blocking observer calls
(`WaitForWaitGroup`, `WaitAtBarrier`). Writes are:
- Locks: `AcquireLock` (blocking) / `ReleaseLock`
- Semaphores: `AcquireSemaphore` (blocking) / `ReleaseSemaphore`
- Wait Groups: `CompleteJobsFromWaitGroup` (mostly) / `UpdateWaitGroup`
- Barriers: `ArriveAtBarrier` (mostly) / `UpdateBarrier`

### Lock-Specific

- `--exclusive-lock-pct` - Percentage of exclusive locks (default: `50`)

### Semaphore-Specific

- `--semaphore-permits` - Initial permits per semaphore (default: `100`)
- `--semaphore-weight-max` - Max weight for acquire (default: `10`)

Workers will acquire between 1 and max weight permits.

### Wait Group-Specific

- `--waitgroup-initial-counter` - Initial counter for wait groups (default: `100`)
- `--waitgroup-job-batch-size` - Jobs to complete (or counter to raise) at once (default: `5`)
- `--waitgroup-expires-in` - How far in the future a wait group's `expires_at` is set (default: `1h`)
- `--waitgroup-delete-after-finished` - Retention of a finished wait group before GC (default: `10s`)

### Barrier-Specific

- `--barrier-expected-processes` - Expected participants per barrier (default: `4`)
- `--barrier-delete-inactive-after` - Auto-delete a barrier after this much inactivity (default: `1h`)

### Blocking Timeouts

- `--acquire-timeout` - Server-side timeout for `AcquireLock`/`AcquireSemaphore` (default: `2s`)
- `--wait-timeout` - Server-side timeout for `WaitForWaitGroup`/`WaitAtBarrier` (default: `5s`)

The client RPC deadline is set comfortably above each server-side timeout.

### General

- `--prometheus-port` - Prometheus metrics port (default: `2113`)
- `--log-interval` - Stats logging interval (default: `5s`)
- `--cleanup` - Cleanup resources on shutdown (default: `true`)

## Workload Scenarios

### 1. Lock-Focused Test

Test high-concurrency lock contention:
```bash
go run . \
  --workers=500 \
  --locks-pct=100 --semaphores-pct=0 --waitgroups-pct=0 --barriers-pct=0 \
  --locks-per-ns=50 \
  --duration=2m
```

### 2. Exclusive Lock Stress Test

Test exclusive lock behavior:
```bash
go run . \
  --workers=200 \
  --locks-pct=100 --semaphores-pct=0 --waitgroups-pct=0 --barriers-pct=0 \
  --exclusive-lock-pct=100 \
  --duration=1m
```

### 3. Shared Lock Test

Test shared lock behavior:
```bash
go run . \
  --workers=300 \
  --locks-pct=100 --semaphores-pct=0 --waitgroups-pct=0 --barriers-pct=0 \
  --exclusive-lock-pct=0 \
  --duration=1m
```

### 4. Semaphore-Focused Test

Test semaphore permit management:
```bash
go run . \
  --workers=200 \
  --semaphores-pct=100 --locks-pct=0 --waitgroups-pct=0 --barriers-pct=0 \
  --semaphore-permits=50 \
  --semaphore-weight-max=5 \
  --duration=2m
```

### 5. Wait Group Test

Test wait group counter operations:
```bash
go run . \
  --workers=100 \
  --waitgroups-pct=100 --locks-pct=0 --semaphores-pct=0 --barriers-pct=0 \
  --waitgroup-initial-counter=1000 \
  --waitgroup-job-batch-size=10 \
  --duration=2m
```

### 6. Barrier Test

Test barrier rendezvous and generation churn:
```bash
go run . \
  --workers=100 \
  --barriers-pct=100 --locks-pct=0 --semaphores-pct=0 --waitgroups-pct=0 \
  --barrier-expected-processes=8 \
  --duration=2m
```

### 7. Mixed Workload

Test realistic mixed workload:
```bash
go run . \
  --workers=300 \
  --locks-pct=30 --semaphores-pct=30 --waitgroups-pct=20 --barriers-pct=20 \
  --read-pct=70 \
  --duration=5m
```

### 8. Rate-Limited Test

Test specific throughput:
```bash
go run . \
  --workers=100 \
  --rate=10000 \
  --duration=2m
```

### 9. Long-Running Soak Test

Test stability over time:
```bash
go run . \
  --workers=50 \
  --duration=1h \
  --log-interval=30s
```

## Metrics

### Prometheus Metrics

Available at `http://localhost:2113/metrics`:

#### Request Metrics
- `load_generator_requests_total{operation,status}` - Total requests by operation and status
- `load_generator_request_duration_seconds{operation}` - Request latency histogram
- `load_generator_errors_total{operation,error_type}` - Errors by type

#### Resource Metrics
- `load_generator_active_workers` - Number of active workers
- `load_generator_acquired_locks` - Currently held locks
- `load_generator_acquired_semaphores` - Currently held semaphores
- `load_generator_inflight_blocking` - Blocking calls (acquire/wait) currently in flight
- `load_generator_blocking_dropped_total{operation}` - Blocking ops skipped because the in-flight cap was reached

#### Throughput
- `load_generator_requests_per_second` - Current RPS (sliding window)

### Console Output

Real-time stats are printed every `--log-interval`:
```
[00:05:00] Requests: 150,000 | Errors: 45 | RPS: 500 | Success: 99.97%
  Locks: 45,000 (30%) | Semaphores: 37,500 (25%) | WaitGroups: 37,500 (25%) | Barriers: 30,000 (20%)
```

Fields:
- **Time**: Elapsed time (HH:MM:SS)
- **Requests**: Total requests completed
- **Errors**: Total errors encountered
- **RPS**: Average requests per second
- **Success**: Success rate percentage
- **Breakdown**: Requests by operation type with percentage

## Cleanup

By default, the load generator cleans up all created resources on shutdown (`--cleanup=true`).

To preserve resources for inspection:
```bash
go run . --cleanup=false
```

Resources created:
- Namespaces: `load-test-ns-{index}`
- Locks: `lock-{index}` (per namespace)
- Semaphores: `sem-{index}` (per namespace)
- Wait Groups: `wg-{index}` (per namespace)
- Barriers: `barrier-{index}` (per namespace)

## Architecture

### Components

- **main.go** - Entry point, orchestration, signal handling
- **config.go** - Configuration and validation
- **worker.go** - Worker goroutine implementation and the blocking-call pool
- **operations.go** - Operation implementations
- **resources.go** - Resource pool management
- **waitgroups.go** - Wait group lifecycle state (jobs, counter, recreation)
- **barriers.go** - Barrier lifecycle state (participants, generations)
- **stats.go** - Statistics collection
- **metrics.go** - Prometheus metrics

### Worker Model

Each worker:
1. Runs in independent goroutine
2. Selects operations based on configured mix
3. Executes non-blocking operations inline against random resources
4. Dispatches blocking operations (acquire/wait) to a shared bounded pool of background
   goroutines so the worker never stalls
5. Tracks acquired resources for release
6. Records metrics and statistics
7. Respects rate limits

### Resource Management

Resources are pre-created at startup:
1. Create namespaces
2. Create semaphores with initial permits
3. Create wait groups with initial counter
4. Create barriers with the expected participant count
5. Generate lock names (locks created on-demand)

Workers randomly select resources from the pool. Wait groups and barriers carry coherent
lifecycle state (see [Resource lifecycle](#resource-lifecycle)).
