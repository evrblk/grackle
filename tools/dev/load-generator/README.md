# Grackle Load Generator

A flexible, high-performance load testing tool for Grackle.

## Overview

The load generator spawns N worker goroutines that generate concurrent requests to a Grackle server, exercising:
- **Locks** (shared and exclusive): acquire, release, and read state
- **Semaphores**: acquire, release, and read state
- **Wait Groups**: add jobs, complete jobs, and read state

The tool provides:
- Configurable workload patterns (operation mix, read/write ratio)
- Real-time statistics and Prometheus metrics
- Graceful shutdown and optional resource cleanup
- Rate limiting for controlled load

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
[00:00:15] Requests: 339,793 | Errors: 0 | RPS: 22,652 | Success: 100.0%
  Locks: 136,290 (40%) | Semaphores: 101,665 (30%) | WaitGroups: 101,838 (30%)
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

### Resources

- `--namespaces` - Number of namespaces to create (default: `5`)
- `--locks-per-ns` - Locks per namespace (default: `100`)
- `--semaphores-per-ns` - Semaphores per namespace (default: `20`)
- `--waitgroups-per-ns` - Wait groups per namespace (default: `10`)

### Operation Mix

Percentages must sum to 100:
- `--locks-pct` - Percentage of lock operations (default: `40`)
- `--semaphores-pct` - Percentage of semaphore operations (default: `30`)
- `--waitgroups-pct` - Percentage of wait group operations (default: `30`)

### Read/Write Ratio

- `--read-pct` - Percentage of read operations for each type (default: `30`)

Applies to all primitive types. Read operations are:
- Locks: `GetLock`
- Semaphores: `GetSemaphore`
- Wait Groups: `GetWaitGroup`

Write operations are randomly split between acquire/release or add/complete.

### Lock-Specific

- `--exclusive-lock-pct` - Percentage of exclusive locks (default: `50`)

### Semaphore-Specific

- `--semaphore-permits` - Initial permits per semaphore (default: `100`)
- `--semaphore-weight-max` - Max weight for acquire (default: `10`)

Workers will acquire between 1 and max weight permits.

### Wait Group-Specific

- `--waitgroup-initial-counter` - Initial counter for wait groups (default: `100`)
- `--waitgroup-job-batch-size` - Jobs to add/complete at once (default: `5`)

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
  --locks-pct=100 --semaphores-pct=0 --waitgroups-pct=0 \
  --locks-per-ns=50 \
  --duration=2m
```

### 2. Exclusive Lock Stress Test

Test exclusive lock behavior:
```bash
go run . \
  --workers=200 \
  --locks-pct=100 --semaphores-pct=0 --waitgroups-pct=0 \
  --exclusive-lock-pct=100 \
  --duration=1m
```

### 3. Shared Lock Test

Test shared lock behavior:
```bash
go run . \
  --workers=300 \
  --locks-pct=100 --semaphores-pct=0 --waitgroups-pct=s0 \
  --exclusive-lock-pct=0 \
  --duration=1m
```

### 4. Semaphore-Focused Test

Test semaphore permit management:
```bash
go run . \
  --workers=200 \
  --semaphores-pct=100 --locks-pct=0 --waitgroups-pct=0 \
  --semaphore-permits=50 \
  --semaphore-weight-max=5 \
  --duration=2m
```

### 5. Wait Group Test

Test wait group counter operations:
```bash
go run . \
  --workers=100 \
  --waitgroups-pct=100 --locks-pct=0 --semaphores-pct=0 \
  --waitgroup-initial-counter=1000 \
  --waitgroup-job-batch-size=10 \
  --duration=2m
```

### 6. Mixed Workload

Test realistic mixed workload:
```bash
go run . \
  --workers=300 \
  --locks-pct=40 --semaphores-pct=35 --waitgroups-pct=25 \
  --read-pct=70 \
  --duration=5m
```

### 7. Rate-Limited Test

Test specific throughput:
```bash
go run . \
  --workers=100 \
  --rate=10000 \
  --duration=2m
```

### 8. Long-Running Soak Test

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

#### Throughput
- `load_generator_requests_per_second` - Current RPS (sliding window)

### Console Output

Real-time stats are printed every `--log-interval`:
```
[00:05:00] Requests: 150,000 | Errors: 45 | RPS: 500 | Success: 99.97%
  Locks: 60,000 (40%) | Semaphores: 45,000 (30%) | WaitGroups: 45,000 (30%)
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

## Architecture

### Components

- **main.go** - Entry point, orchestration, signal handling
- **config.go** - Configuration and validation
- **worker.go** - Worker goroutine implementation
- **operations.go** - Operation implementations
- **resources.go** - Resource pool management
- **stats.go** - Statistics collection
- **metrics.go** - Prometheus metrics

### Worker Model

Each worker:
1. Runs in independent goroutine
2. Selects operations based on configured mix
3. Executes operations against random resources
4. Tracks acquired resources for release
5. Records metrics and statistics
6. Respects rate limits

### Resource Management

Resources are pre-created at startup:
1. Create namespaces
2. Create semaphores with initial permits
3. Create wait groups with initial counter
4. Generate lock names (locks created on-demand)

Workers randomly select resources from pool.
