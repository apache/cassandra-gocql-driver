#!/bin/bash

set -e

function run_tests() {
	local clusterSize=3
	local version=$1

	ccm create test -v binary:$version -n $clusterSize -d --vnodes
	
	sed -i '/#MAX_HEAP_SIZE/c\MAX_HEAP_SIZE="256M"' ~/.ccm/repository/$version/conf/cassandra-env.sh
	sed -i '/#HEAP_NEWSIZE/c\HEAP_NEWSIZE="100M"' ~/.ccm/repository/$version/conf/cassandra-env.sh

	ccm updateconf 'concurrent_reads: 2' 'concurrent_writes: 2' 'rpc_server_type: sync' 'rpc_min_threads: 2' 'rpc_max_threads: 2' 'write_request_timeout_in_ms: 5000' 'read_request_timeout_in_ms: 5000'
	ccm start
	ccm status

	local proto=2
	if [[ $version == 1.2.* ]]; then
		proto=1
	fi

	go test -v -proto=$proto -rf=3 -cluster=$(ccm liveset) -clusterSize=$clusterSize -autowait=2000ms ./...

	ccm clear
}

run_tests $1
