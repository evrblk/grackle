# compose-cluster

A local Docker Compose Grackle/Monstera cluster for development: three provisioned
nodes, a fourth **unprovisioned** node, a gateway, a worker, Prometheus and Grafana.

Nodes self-provision from their data dir — on first boot the compose command stages
`config/cluster.json` + `config/node.json` into `.data/node-N`, so `node-1..3` start
already `READY` (no bootstrap RPC). `node-4` stages nothing, so it comes up
`UNPROVISIONED` and waits to be added to the cluster. State persists in `.data/`
across restarts (delete it to start fresh).

| Service | Port | Notes |
|---|---|---|
| node-1 / node-2 / node-3 | 8001 / 8002 / 8003 | provisioned from staged data dir |
| node-4 | 8004 | starts UNPROVISIONED |
| gateway | 9000 | Grackle API |
| prometheus | 9090 | metrics |
| grafana | 3000 | dashboards (anonymous admin) |

**Prerequisites:** Docker (with Compose) and Go. Run everything from this directory.

## 1. Generate the cluster config

```sh
./cluster_config.sh          # writes cluster_config.json: node-1..3 + Grackle apps
```

## 2. Start the cluster from scratch

```sh
rm -rf .data                 # optional: wipe any previous state
./compose-up.sh -d           # builds the grackle binary + image, then `docker compose up -d`
```

Omit `-d` to run in the foreground and watch logs. `compose-up.sh` forwards its
arguments to `docker compose up`.

## 3. Verify it is running

```sh
docker compose ps                              # node-1..4, gateway, worker, etc. Up
docker compose logs node-1 | grep -i ready     # "Node is ready"
```

Node-1..3 should be `READY`; node-4 logs `Node is unprovisioned; awaiting Bootstrap`.
Open Prometheus (http://localhost:9090) and check `monstera_node_ready` (1 for
node-1..3, absent/0 for node-4) and `monstera_config_version_number`, or use Grafana
(http://localhost:3000).

## 4. Add node-4 (update the config + bootstrap it) with the CLI

`monstera cluster add-node` plans a new config (version + 1, with node-4 added),
rolls it out to node-1..3, and bootstraps node-4 — in one command. It must run
**inside the compose network** so the `node-N` addresses resolve, so we build the
`monstera` binary and run it in a throwaway container on that network:

```sh
# Build a linux monstera binary next to the grackle one.
CGO_ENABLED=0 GOOS=linux go build -o monstera github.com/evrblk/monstera/cmd/monstera

# Run add-node on the compose network (reuses the gateway service's image + network).
docker compose run --rm --no-deps \
  -v "$PWD/monstera:/grackle/monstera" \
  --entrypoint /grackle/monstera \
  gateway cluster add-node \
    --config cluster_config.json \
    --node-id node-4 --node-address node-4:8004
```

The command finishes with `Node "node-4" added; cluster is at version 7.` It is
idempotent and resumable — safe to re-run if interrupted.

## 5. Ensure node-4 is up and joined

```sh
docker compose logs node-4 | grep -iE "bootstrap|ready"   # "Node bootstrapped ... ready"
```

In Prometheus, confirm `monstera_node_ready{node="node-4"} == 1` and that
`monstera_config_version_number` reads the new version (7) on **all four** nodes —
that is the whole cluster converged on the config that includes node-4. node-4
holds no shard replicas yet — move one onto it in step 6.

## 6. Move a shard's replica onto node-4

`add-node` bumped the live config to version 7, and control commands don't write it
locally (the running cluster is the source of truth). So first download the current
config with `monstera cluster get-config`, then move a shard against it. Both run on
the compose network, like add-node:

```sh
# 6a. Download the live config (v7) to a local file. get-config prints the config to
#     stdout (progress goes to stderr), so the redirect is clean. -T disables the TTY.
docker compose run --rm --no-deps -T \
  -v "$PWD/monstera:/grackle/monstera" \
  --entrypoint /grackle/monstera \
  gateway cluster get-config --node-address node-1:8001 > current_config.json

# 6b. Pick a shard and move its replica from node-1 to node-4.
SHARD=$(jq -r '.applications[0].shards[0].id' current_config.json)   # e.g. GrackleLocks_00_0f
docker compose run --rm --no-deps \
  -v "$PWD/monstera:/grackle/monstera" \
  -v "$PWD/current_config.json:/grackle/current_config.json:ro" \
  --entrypoint /grackle/monstera \
  gateway cluster move-shard \
    --config current_config.json \
    --shard-id "$SHARD" --from-node node-1 --to-node node-4 --bake 15s
```

move-shard adds a replica on node-4, waits for catch-up, bakes for `--bake`, then
removes node-1's replica — finishing with `... cluster is at version 9.` (add = v8,
bake, remove = v9). It is checkpointed and resumable.

## 7. Ensure the shard moved

Re-fetch the live config and pipe it straight to jq:

```sh
docker compose run --rm --no-deps -T \
  -v "$PWD/monstera:/grackle/monstera" \
  --entrypoint /grackle/monstera \
  gateway cluster get-config --node-address node-1:8001 \
  | jq --arg s "$SHARD" '.applications[].shards[] | select(.id==$s) | .replicas[].node_id'
```

The shard's replicas should now be on node-2, node-3 and node-4 (node-1 gone), and
Prometheus `monstera_config_version_number` should read 9 on all nodes.

## Teardown

```sh
./compose-down.sh            # docker compose down --remove-orphans
rm -rf .data                 # wipe node state (forces re-provision next time)
```

## Notes

- The staged `cluster_config.json` is a **first-boot seed only**. Once a node is
  provisioned it owns `config/cluster.json` and rewrites it on config changes, so
  editing the seed file does nothing for a running node — change topology with the
  `monstera cluster` commands (or `UpdateClusterConfig`) instead.
- After step 4, all four nodes persist the new config in `.data`, so a later
  `./compose-up.sh` brings the full 4-node cluster back provisioned.
