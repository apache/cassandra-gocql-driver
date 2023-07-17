#!/bin/bash
#
# Copyright (C) 2017 ScyllaDB
#

readonly SCYLLA_IMAGE=${SCYLLA_IMAGE}

set -eu -o pipefail

function scylla_up() {
  local -r exec="docker compose exec -T"

  echo "==> Running Scylla ${SCYLLA_IMAGE}"
  docker pull ${SCYLLA_IMAGE}
  docker compose up -d --wait
}

function scylla_down() {
  echo "==> Stopping Scylla"
  docker compose down
}

function scylla_restart() {
  scylla_down
  scylla_up
}

scylla_restart

readonly clusterSize=1
readonly scylla_liveset="192.168.100.11"
readonly cversion="3.11.4"
readonly proto=4
readonly args="-gocql.timeout=60s -proto=${proto} -rf=${clusterSize} -clusterSize=${clusterSize} -autowait=2000ms -compressor=snappy -gocql.cversion=${cversion} -cluster=${scylla_liveset}"

echo "==> Running $* tests with args: ${args}"
go test -timeout=5m -race -tags="$*" ${args} ./...
