#!/bin/bash

set -e

function run_tests() {
	local clusterSize=3
	local localNodes=2
	local localDC=dc1
	local version=$1

	ccm create test -v binary:$version -d
	
	sed -i '/#MAX_HEAP_SIZE/c\MAX_HEAP_SIZE="256M"' ~/.ccm/repository/$version/conf/cassandra-env.sh
	sed -i '/#HEAP_NEWSIZE/c\HEAP_NEWSIZE="100M"' ~/.ccm/repository/$version/conf/cassandra-env.sh

	ccm updateconf 'client_encryption_options.enabled: true' 'client_encryption_options.keystore: testdata/pki/.keystore' 'client_encryption_options.keystore_password: cassandra' 'client_encryption_options.require_client_auth: true' 'client_encryption_options.truststore: testdata/pki/.truststore' 'client_encryption_options.truststore_password: cassandra' 'concurrent_reads: 2' 'concurrent_writes: 2' 'rpc_server_type: sync' 'rpc_min_threads: 2' 'rpc_max_threads: 2' 'write_request_timeout_in_ms: 5000' 'read_request_timeout_in_ms: 5000'
	ccm populate -n $localNodes:$(($clusterSize-$localNodes)) --vnodes	

	ccm start
	ccm status

	local proto=2
	if [[ $version == 1.2.* ]]; then
		proto=1
	fi

	go test -cover -v -runssl -proto=$proto -rf=3 -cluster=$(ccm liveset) -clusterSize=$clusterSize -autowait=2000ms -localNodes $localNodes -localDC $localDC ./... > results

	cat results
	cover=`cat results | grep coverage: | grep -o "[0-9]\{1,3\}" | head -n 1`
	if [[ $cover -lt "64" ]]; then
		echo "--- FAIL: expected coverage of at least 64 %, but coverage was $cover %"
		exit 1
	fi
	ccm clear
}
run_tests $1
