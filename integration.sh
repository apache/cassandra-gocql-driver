#!/bin/bash

set -e

function run_tests() {
	local version=$1
	ccm create test -v $version -n 3 -s -d --vnodes
	ccm status
	ccm updateconf 'concurrent_reads: 8' 'concurrent_writes: 32' 'rpc_server_type: sync' 'rpc_min_threads: 2' 'rpc_max_threads: 8' 'write_request_timeout_in_ms: 5000' 'read_request_timeout_in_ms: 5000'
 
	ccm node1 nodetool disableautocompaction
	ccm node2 nodetool disableautocompaction
	ccm node3 nodetool disableautocompaction

	local proto=2
	if [[ $version == 1.2.* ]]; then
		proto=1
	fi

	go test -v -proto=$proto -rf=3 -cluster=$(ccm liveset) ./...

	ccm clear
}

run_tests $1
