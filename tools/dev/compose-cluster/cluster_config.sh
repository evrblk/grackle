#!/usr/bin/env bash

set -e

go tool github.com/evrblk/monstera/cmd/monstera config init \
  --node-id=node-1 --node-address=node-1:8001 \
  --node-id=node-2 --node-address=node-2:8002 \
  --node-id=node-3 --node-address=node-3:8003 \
  --output=./cluster_config.json

go tool github.com/evrblk/monstera/cmd/monstera config add-application \
  --config=./cluster_config.json \
  --name=GrackleLocks \
  --implementation=GrackleLocks \
  --shards-count=16

go tool github.com/evrblk/monstera/cmd/monstera config add-application \
  --config=./cluster_config.json \
  --name=GrackleSemaphores \
  --implementation=GrackleSemaphores \
  --shards-count=16

go tool github.com/evrblk/monstera/cmd/monstera config add-application \
  --config=./cluster_config.json \
  --name=GrackleWaitGroups \
  --implementation=GrackleWaitGroups \
  --shards-count=16

go tool github.com/evrblk/monstera/cmd/monstera config add-application \
  --config=./cluster_config.json \
  --name=GrackleBarriers \
  --implementation=GrackleBarriers \
  --shards-count=16

go tool github.com/evrblk/monstera/cmd/monstera config add-application \
  --config=./cluster_config.json \
  --name=GrackleNamespaces \
  --implementation=GrackleNamespaces \
  --shards-count=8
