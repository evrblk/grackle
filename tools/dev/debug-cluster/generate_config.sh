#!/usr/bin/env bash

set -e

go tool github.com/evrblk/monstera/cmd/monstera config init \
  --node-id=node-1 --node-address=localhost:8001 \
  --node-id=node-2 --node-address=localhost:8002 \
  --node-id=node-3 --node-address=localhost:8003 \
  --node-id=node-4 --node-address=localhost:8004 \
  --node-id=node-5 --node-address=localhost:8005 \
  --output=./cluster_config.json

go tool github.com/evrblk/monstera/cmd/monstera config add-application \
  --config=./cluster_config.json \
  --name=GrackleLocks \
  --implementation=GrackleLocks \
  --shards-count=32

go tool github.com/evrblk/monstera/cmd/monstera config add-application \
  --config=./cluster_config.json \
  --name=GrackleSemaphores \
  --implementation=GrackleSemaphores \
  --shards-count=32

go tool github.com/evrblk/monstera/cmd/monstera config add-application \
  --config=./cluster_config.json \
  --name=GrackleWaitGroups \
  --implementation=GrackleWaitGroups \
  --shards-count=32

go tool github.com/evrblk/monstera/cmd/monstera config add-application \
  --config=./cluster_config.json \
  --name=GrackleBarriers \
  --implementation=GrackleBarriers \
  --shards-count=32

go tool github.com/evrblk/monstera/cmd/monstera config add-application \
  --config=./cluster_config.json \
  --name=GrackleNamespaces \
  --implementation=GrackleNamespaces \
  --shards-count=16
