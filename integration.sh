#!/bin/bash

set -e

PID_FILE=cassandra.pid
STARTUP_LOG=startup.log

for v in 2.0.6 2.0.7
do
	TARBALL=apache-cassandra-$v-bin.tar.gz
	CASSANDRA_DIR=apache-cassandra-$v

	curl -L -O -C - http://archive.apache.org/dist/cassandra/$v/$TARBALL
	
	if [ ! -f $CASSANDRA_DIR/bin/cassandra ]
	then
   		tar xzf $TARBALL
	fi
	
	CASSANDRA_LOG_DIR=`pwd`/v${v}/log/cassandra
	CASSANDRA_LOG=$CASSANDRA_LOG_DIR/system.log

	mkdir -p $CASSANDRA_LOG_DIR
	: >$CASSANDRA_LOG  # create an empty log file
	
	sed -i -e 's?/var?'`pwd`/v${v}'?' $CASSANDRA_DIR/conf/cassandra.yaml
	sed -i -e 's?/var?'`pwd`/v${v}'?' $CASSANDRA_DIR/conf/log4j-server.properties

	echo "Booting Cassandra ${v}, waiting for CQL listener to start ...."

	$CASSANDRA_DIR/bin/cassandra -p $PID_FILE &> $STARTUP_LOG

	{ tail -n +1 -f $CASSANDRA_LOG & } | sed -n '/Starting listening for CQL clients/q'
	
	PID=$(<"$PID_FILE")

	echo "Cassandra ${v} running (PID ${PID}), about to run test suite ...."

	go test -v ./...

	echo "Test suite passed against Cassandra ${v}, killing server instance (PID ${PID})"
	
	kill -9 $PID
	rm $PID_FILE
done