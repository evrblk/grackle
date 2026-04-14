# Debug Cluster

A development tool that runs a complete Grackle cluster (nodes + gateway) in a single process for local debugging and testing.

## Overview

This tool simplifies local development by running a complete Grackle cluster in one process, eliminating the need for Docker Compose or multiple terminal windows. It includes:
- All Monstera nodes defined in the cluster configuration
- API gateway for client connections
- All nodes communicate via localhost (gRPC) or in-memory (local transport)

## Setup

### 1. Generate Cluster Configuration

First, generate the cluster configuration file:

```bash
./generate_config.sh
```

This creates `cluster_config.json` with 5 nodes. The configuration includes all Grackle applications.

### 2. Build the Binary

```bash
go build -o debug-cluster .
```

## Usage

### Start the Cluster

Run all nodes in a single process:

```bash
./debug-cluster
```

Optional flags:
- `--config` - Path to cluster config file (default: `./cluster_config.json`)
- `--data-dir` - Base directory for node data (default: `./.data`)
- `--prometheus-port` - Prometheus metrics port (default: `2112`)
- `--cpu-profile` - Write CPU profile to file (e.g., `cpu.prof`)
- `--transport` - Transport type: `grpc` or `local` (default: `grpc`)
- `--gateway-port` - Gateway port for client connections (default: `0` = disabled)

Example (gateway + local transport + profiling):

```bash
./debug-cluster --gateway-port=9000 --transport=local --cpu-profile=cpu.prof
```

### Stop the Cluster

Press `Ctrl+C` to gracefully shut down all nodes.

### Clean Up Data

Remove all node data:

```bash
rm -rf .data/
```

## Transport Types

The debug cluster supports two transport modes for inter-node communication:

### gRPC Transport (default)

Uses real gRPC connections over localhost network interfaces. This mode:
- Simulates production networking behavior
- Uses actual TCP/IP stack
- Each node runs a gRPC server on its configured port
- Better for testing network-related behavior
- Slightly higher overhead due to serialization and network stack

```bash
./debug-cluster --transport=grpc
```

### Local Transport

Uses in-memory function calls between nodes. This mode:
- Eliminates network overhead completely
- All communication happens via direct function calls
- No gRPC servers are started
- Significantly faster for high-throughput scenarios
- Perfect for performance testing and profiling
- Ideal for rapid development iteration

```bash
./debug-cluster --transport=local
```

## Monitoring

Prometheus metrics are exposed at `http://localhost:2112/metrics` (or your custom port).

## Profiling

### CPU Profiling

Enable CPU profiling to analyze performance:

```bash
./debug-cluster --cpu-profile=cpu.prof
```

The profiler will run until you stop the cluster with `Ctrl+C`. Analyze the profile with:

```bash
go tool pprof cpu.prof
```

### Architecture

```
┌─────────────────────────────────────────────────┐
│         Single Process (debug-cluster)          │
│                                                 │
│  ┌────────────────────────────────────────────┐ │
│  │  Gateway (optional, :9000)                 │ │
│  │  - Accepts client gRPC requests            │ │
│  │  - Routes to shards automatically          │ │
│  └────────────┬───────────────────────────────┘ │
│               │                                 │
│               │ Transport (gRPC or local)       │
│               ▼                                 │
│  ┌────────────────────────────────────────────┐ │
│  │  Monstera Nodes                            │ │
│  │  - node-1 (:8001) │ node-2 (:8002) │ ...   │ │
│  │  - Each hosts multiple shard replicas      │ │
│  └────────────────────────────────────────────┘ │
│                                                 │
│  ┌────────────────────────────────────────────┐ │
│  │  Prometheus Metrics (:2112)                │ │
│  └────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────┘
```
