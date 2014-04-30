#!/bin/bash

set -e

PID_FILE=cassandra.pid
STARTUP_LOG=startup.log
CASSANDRA_LOG=cassandra.log

for v in 2.0.7
do
	TARBALL=apache-cassandra-$v-bin.tar.gz
	CASSANDRA_DIR=apache-cassandra-$v

	curl -L -O -C - ftp://ftp.mirrorservice.org/sites/ftp.apache.org/cassandra/$v/$TARBALL
	
	if [ ! -f $CASSANDRA_DIR/bin/cassandra ]
	then
   		tar xzf $TARBALL
	fi
	
	cp log4j-server.properties $CASSANDRA_DIR/conf
	: >$CASSANDRA_LOG  # create an empty log file

	echo "Booting Cassandra ${v}"

	apache-cassandra-2.0.7/bin/cassandra -p $PID_FILE &> $STARTUP_LOG
	# { tail -n +1 -f $CASSANDRA_LOG & } | sed -n '/Starting listening for CQL clients/q'

	sleep 30
	tail $CASSANDRA_LOG

	PID=$(<"$PID_FILE")

	echo "Cassandra ${v} running (PID ${PID}), about to run test suite ...."

	go test -v ./...

	echo "Test suite passed against Cassandra ${v}, killing server instance (PID ${PID})"
	
	kill -9 $PID
	rm $PID_FILE
done