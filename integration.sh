#!/bin/bash

set -e

function vercomp() {
    if [[ $1 == $2 ]]
    then
        return 0
    fi
    local IFS=.
    local i ver1=($1) ver2=($2)
    # fill empty fields in ver1 with zeros
    for ((i=${#ver1[@]}; i<${#ver2[@]}; i++))
    do
        ver1[i]=0
    done
    for ((i=0; i<${#ver1[@]}; i++))
    do
        if [[ -z ${ver2[i]} ]]
        then
            # fill empty fields in ver2 with zeros
            ver2[i]=0
        fi
        if ((10#${ver1[i]} > 10#${ver2[i]}))
        then
            return 1
        fi
        if ((10#${ver1[i]} < 10#${ver2[i]}))
        then
            return 2
        fi
    done
    return 0
}

function run_tests() {
	local clusterSize=3
	local version=$1

	ccm create test -v binary:$version -n $clusterSize -d --vnodes

	sed -i '/#MAX_HEAP_SIZE/c\MAX_HEAP_SIZE="256M"' ~/.ccm/repository/$version/conf/cassandra-env.sh
	sed -i '/#HEAP_NEWSIZE/c\HEAP_NEWSIZE="100M"' ~/.ccm/repository/$version/conf/cassandra-env.sh

	ccm updateconf "client_encryption_options.enabled: true"
	ccm updateconf "client_encryption_options.keystore: testdata/pki/.keystore"
	ccm updateconf "client_encryption_options.keystore_password: cassandra"
	ccm updateconf "client_encryption_options.require_client_auth: true"
	ccm updateconf "client_encryption_options.truststore: testdata/pki/.truststore"
	ccm updateconf "client_encryption_options.truststore_password: cassandra"
	ccm updateconf "concurrent_reads: 2"
	ccm updateconf "concurrent_writes: 2"
	ccm updateconf "rpc_server_type: sync"
	ccm updateconf "rpc_min_threads: 2"
	ccm updateconf "rpc_max_threads: 2"
	ccm updateconf "write_request_timeout_in_ms: 5000"
	ccm updateconf "read_request_timeout_in_ms: 5000"

	# ccm updateconf 'client_encryption_options.enabled: true' 'client_encryption_options.keystore: testdata/pki/.keystore' 'client_encryption_options.keystore_password: cassandra' 'client_encryption_options.require_client_auth: true' 'client_encryption_options.truststore: testdata/pki/.truststore' 'client_encryption_options.truststore_password: cassandra' 'concurrent_reads: 2' 'concurrent_writes: 2' 'rpc_server_type: sync' 'rpc_min_threads: 2' 'rpc_max_threads: 2' 'write_request_timeout_in_ms: 5000' 'read_request_timeout_in_ms: 5000'
	# ccm start -v
	# ccm status
	# ccm node1 nodetool status

	local proto=2
	if [[ $version == 1.2.* ]]; then
		proto=1
	fi

	# go test -timeout 5m -tags integration -cover -v -runssl -proto=$proto -rf=3 -cluster=$(ccm liveset) -clusterSize=$clusterSize -autowait=2000ms ./... | tee results.txt

	# if [ ${PIPESTATUS[0]} -ne 0 ]; then 
	# 	echo "--- FAIL: ccm status follows:"
	# 	ccm status
	# 	ccm node1 nodetool status
	# 	ccm node1 showlog > status.log
	# 	cat status.log
	# 	echo "--- FAIL: Received a non-zero exit code from the go test execution, please investigate this"
	# 	exit 1
	# fi

	# cover=`cat results.txt | grep coverage: | grep -o "[0-9]\{1,3\}" | head -n 1`

	# if [[ $cover -lt "55" ]]; then
	# 	echo "--- FAIL: expected coverage of at least 60 %, but coverage was $cover %"
	# 	exit 1
	# fi

	# ccm stop
	# ccm status

	local miniumum=2.1
	vercomp $version $miniumum

    if [[ $? == 1 ]]
    then
    	ccm updateconf "authenticator: PasswordAuthenticator"
		ccm updateconf "authorizer: CassandraAuthorizer"

		ccm start -v
		ccm status

		go test -v . -timeout 10s -run=TestAuthentication -tags integration -runauth -proto=$proto -cluster=$(ccm liveset) -clusterSize=$clusterSize -autowait=2000ms    
    else
        echo "Ignoring authentication test for Cassandra version $version"
    fi

	ccm clear
}
run_tests $1
