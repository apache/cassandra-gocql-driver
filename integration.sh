#!/bin/bash
#
# Copyright (C) 2017 ScyllaDB
#

readonly SCYLLA_IMAGE=${SCYLLA_IMAGE}

set -eu -o pipefail

# Static IPs from docker-compose.yml
scylla_liveset="192.168.100.11,192.168.100.12"

function scylla_up() {
  local -r exec="docker-compose exec -T"

  echo "==> Running Scylla ${SCYLLA_IMAGE}"
  docker pull ${SCYLLA_IMAGE}
  docker-compose up -d

  echo "==> Waiting for CQL port"
  for s in $(docker-compose ps --services); do
    until v=$(${exec} ${s} cqlsh -e "DESCRIBE SCHEMA"); do
      echo ${v}
      docker-compose logs --tail 10 ${s}
      sleep 5
    done
  done
  echo "==> Waiting for CQL port done"
}

function scylla_down() {
  echo "==> Stopping Scylla"
  docker-compose down
}

function scylla_restart() {
  scylla_down
  scylla_up
}

scylla_restart

readonly clusterSize=2
readonly cversion="3.11.4"
readonly proto=4
readonly args="-gocql.timeout=60s -proto=${proto} -rf=3 -clusterSize=${clusterSize} -autowait=2000ms -compressor=snappy -gocql.cversion=${cversion} -cluster=${scylla_liveset}"

echo "==> Running $* tests with args: ${args}"
go test -timeout=5m -race -tags="$*" ${args} ./...
