#!/usr/bin/env bash

set -e

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

SRC_DEST="${SCRIPT_DIR}/.src/"
# sync all sources for dlv
rm -Rf "${SRC_DEST}"
mkdir "${SRC_DEST}"
for d in cmd pkg
do
    cp -Rf "${SCRIPT_DIR}/../../../${d}/" "${SRC_DEST}/${d}/"
done

# build -gcflags "all=-N -l" disables optimizations that allow for better run with combination with Delve debugger.
CGO_ENABLED=0 GOOS=linux go build -gcflags "all=-N -l" -o "${SCRIPT_DIR}/grackle" "${SCRIPT_DIR}/../../../cmd/grackle"
# build the compose image
docker compose -f "${SCRIPT_DIR}"/docker-compose.yml build node-1
# cleanup sources
rm -Rf "${SRC_DEST}"
docker compose -f "${SCRIPT_DIR}"/docker-compose.yml up "$@"
