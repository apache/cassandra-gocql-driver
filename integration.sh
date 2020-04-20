#!/bin/bash

set -eux

function run_tests() {
	local clusterSize=3
	local version=$1
	local auth=$2

	if [ "$auth" = true ]; then
		clusterSize=1
	fi

	local keypath="$(pwd)/testdata/pki"

	local conf=(
		"client_encryption_options.enabled: true"
		"client_encryption_options.keystore: $keypath/.keystore"
		"client_encryption_options.keystore_password: cassandra"
		"client_encryption_options.require_client_auth: true"
		"client_encryption_options.truststore: $keypath/.truststore"
		"client_encryption_options.truststore_password: cassandra"
		"concurrent_reads: 2"
		"concurrent_writes: 2"
		"rpc_server_type: sync"
		"rpc_min_threads: 2"
		"rpc_max_threads: 2"
		"write_request_timeout_in_ms: 5000"
		"read_request_timeout_in_ms: 5000"
	)

	ccm remove test || true

	ccm create test -v $version -n $clusterSize -d --vnodes --jvm_arg="-Xmx256m -XX:NewSize=100m"
	ccm updateconf "${conf[@]}"

	if [ "$auth" = true ]
	then
		ccm updateconf 'authenticator: PasswordAuthenticator' 'authorizer: CassandraAuthorizer'
		rm -rf $HOME/.ccm/test/node1/data/system_auth
	fi

	local proto=2
	if [[ $version == 1.2.* ]]; then
		proto=1
	elif [[ $version == 2.0.* ]]; then
		proto=2
	elif [[ $version == 2.1.* ]]; then
		proto=3
	elif [[ $version == 2.2.* || $version == 3.0.* ]]; then
		proto=4
		ccm updateconf 'enable_user_defined_functions: true'
		export JVM_EXTRA_OPTS=" -Dcassandra.test.fail_writes_ks=test -Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler"
	elif [[ $version == 3.*.* ]]; then
		proto=5
		ccm updateconf 'enable_user_defined_functions: true'
		export JVM_EXTRA_OPTS=" -Dcassandra.test.fail_writes_ks=test -Dcassandra.custom_query_handler_class=org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler"
	fi

	sleep 1s

	ccm list
	ccm start --wait-for-binary-proto
	ccm status
	ccm node1 nodetool status

	local args="-gocql.timeout=60s -runssl -proto=$proto -rf=3 -clusterSize=$clusterSize -autowait=2000ms -compressor=snappy -gocql.cversion=$version -cluster=$(ccm liveset) ./..."

	go test -v -tags unit -race

	if [ "$auth" = true ]
	then
		sleep 30s
		go test -run=TestAuthentication -tags "integration gocql_debug" -timeout=15s -runauth $args
	else
		sleep 1s
		go test -tags "cassandra gocql_debug" -timeout=5m -race $args

		ccm clear
		ccm start --wait-for-binary-proto
		sleep 1s

		go test -tags "integration gocql_debug" -timeout=5m -race $args

		ccm clear
		ccm start --wait-for-binary-proto
		sleep 1s

		go test -tags "ccm gocql_debug" -timeout=5m -race $args
	fi

	ccm remove
}

function join_by {
  local IFS="$1";
  shift;
  echo "$*";
}

scylla_liveset="unset"

function startup_scylla {
  local version=$1
  #service_names+=(node_1 node_2 node_3)
  service_names+=(node_1 node_2)
  #container_names+=(gocql_node_1_1 gocql_node_2_1 gocql_node_3_1)
  container_names+=(gocql_node_1_1 gocql_node_2_1)
  SCYLLA_IMAGE=$version docker-compose --log-level WARNING up -d

  for name in "${container_names[@]}"
  do
    node_ips+=( `docker inspect --format='{{ .NetworkSettings.Networks.gocql_public.IPAddress }}' $name` )
  done

  # Wait for instance to start
  for name in "${service_names[@]}"
  do
    until docker-compose logs "${name}"| grep "Starting listening for CQL clients" > /dev/null; do sleep 2; done
  done

  scylla_liveset=$(join_by ',' "${node_ips[@]}")
}

function run_scylla_tests() {
  local version=$1
  local auth=$2
  echo "Running integration tests on ${version}"
  local clusterSize=2
  local cversion="3.11.4"
  startup_scylla "${version}"
  local proto=4
	go test -v -tags unit -race

  if [ "$auth" = true ]
	then
	  :
		#sleep 30s
		#go test -run=TestAuthentication -tags "integration gocql_debug" -timeout=15s -runauth $args
	else
		sleep 1s
	  local args="-gocql.timeout=60s -proto=$proto -rf=3 -clusterSize=$clusterSize -autowait=2000ms -compressor=snappy -gocql.cversion=$cversion -cluster=${scylla_liveset} ./..."
		go test -tags "cassandra scylla gocql_debug" -timeout=5m -race $args
		cleanup_scylla
		startup_scylla "${version}"
		go test -tags "integration scylla gocql_debug" -timeout=5m -race $args
		cleanup_scylla
		startup_scylla "${version}"
		go test -tags "ccm gocql_debug" -timeout=5m -race $args
		cleanup_scylla
	fi
}

function cleanup_scylla {
  echo "Removing scylla"
  docker-compose down
}

if [[ $1 =~ "scylla" ]]
then
  run_scylla_tests $1 $2
else
  run_tests $1 $2
fi
