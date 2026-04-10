# Debug Cluster

A development tool that runs multiple Monstera nodes in a single process for local debugging and testing.

## Overview

This tool simplifies local development by running a complete Grackle cluster in one process, eliminating the need for Docker Compose or multiple terminal windows. All nodes defined in the cluster configuration communicate via localhost on different ports.

## Prerequisites

- Go 1.26 or later
- Monstera CLI tools installed (`github.com/evrblk/monstera/cmd/monstera`)

## Setup

### 1. Generate Cluster Configuration

First, generate the cluster configuration file:

```bash
./generate_config.sh
```

This creates `cluster_config.json` with 5 nodes:
- node-1: localhost:8001
- node-2: localhost:8002
- node-3: localhost:8003
- node-4: localhost:8004
- node-5: localhost:8005

The configuration includes all Grackle applications:
- GrackleLocks (32 shards)
- GrackleSemaphores (32 shards)
- GrackleWaitGroups (32 shards)
- GrackleBarriers (32 shards)
- GrackleNamespaces (16 shards)

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

Example with custom settings:

```bash
./debug-cluster --config ./my_config.json --data-dir ./my_data --prometheus-port 9090
```

Example for performance testing (local transport + CPU profiling):

```bash
./debug-cluster --transport local --cpu-profile cpu.prof
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
./debug-cluster --transport grpc
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
./debug-cluster --transport local
```

**Recommendation**: Use `local` transport for CPU profiling and performance testing to eliminate network overhead from your measurements. Use `grpc` transport when you need to test network behavior or want production-like communication patterns.

## Monitoring

Prometheus metrics are exposed at `http://localhost:2112/metrics` (or your custom port).

## Profiling

### CPU Profiling

Enable CPU profiling to analyze performance:

```bash
./debug-cluster --cpu-profile cpu.prof
```

The profiler will run until you stop the cluster with `Ctrl+C`. Analyze the profile with:

```bash
go tool pprof cpu.prof
```

Common pprof commands:
- `top` - Show top CPU consumers
- `list <function>` - Show annotated source for a function
- `web` - Open interactive graph in browser (requires Graphviz)
- `pdf` - Generate PDF call graph

Example analysis session:

```bash
# Interactive mode
go tool pprof cpu.prof

# Generate SVG call graph
go tool pprof -svg cpu.prof > profile.svg

# Show top 20 functions
go tool pprof -top cpu.prof
```

## Structure

- `main.go` - Main application that runs all nodes from cluster config concurrently
- `generate_config.sh` - Script to generate cluster configuration (currently creates 5 nodes)
- `cluster_config.json` - Generated cluster configuration (git-ignored)
- `.data/` - Data directories for each node (git-ignored)
  - `node-1/` through `node-5/` - Data for each node

## Differences from compose-cluster

- **Single Process**: All nodes run in one process vs separate containers
- **Transport Options**: Choose between gRPC (network) or local (in-memory) transport
- **Localhost**: When using gRPC, nodes communicate via localhost vs Docker network
- **No Docker**: No Docker or Docker Compose required
- **Faster Startup**: Quicker iteration for development and debugging
- **Single Metrics Port**: All nodes share one Prometheus endpoint
- **Better Profiling**: CPU profiling captures all nodes in a single profile

## Use Cases

- **Local development and testing** - Run a full cluster on your machine
- **Debugging cluster behavior** - Single process makes debugging easier
- **Performance testing** - Use local transport to eliminate network overhead
- **CPU profiling** - Profile all nodes together in one process
- **Quick iteration** - No Docker overhead, instant startup
- **Learning** - Understand how Monstera clustering works
- **Integration testing** - Test distributed coordination primitives locally
