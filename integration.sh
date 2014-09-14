#!/bin/bash

set -e

function run_tests() {
	local clusterSize=3
	local localNodes=2
	local localDC=dc1
	local version=$1

	ccm create test -v binary:$version -d
	ccm updateconf 'concurrent_reads: 8' 'concurrent_writes: 32' 'rpc_server_type: sync' 'rpc_min_threads: 2' 'rpc_max_threads: 8' 'write_request_timeout_in_ms: 5000' 'read_request_timeout_in_ms: 5000'
	ccm populate -n $localNodes:$(($clusterSize-$localNodes)) --vnodes
	ccm start
	ccm status

	local proto=2
	if [[ $version == 1.2.* ]]; then
		proto=1
	fi

	go test -v -proto=$proto -rf=3 -cluster=$(ccm liveset) -clusterSize=$clusterSize -autowait=2000ms -localNodes $localNodes -localDC $localDC ./...

	ccm clear
}

run_tests $1
