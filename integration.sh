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
	#sed -i -e 's/\/var/\/tmp/' $CASSANDRA_DIR/conf/cassandra.yaml
	sed -i -e 's?/var?'`pwd`'?' $CASSANDRA_DIR/conf/cassandra.yaml

	echo "Booting Cassandra ${v}, waiting for CQL listener ...."

	$CASSANDRA_DIR/bin/cassandra -p $PID_FILE &> $STARTUP_LOG
	
	sleep 10
	tail -5 $CASSANDRA_LOG

	{ tail -n +1 -f $CASSANDRA_LOG & } | sed -n '/Starting listening for CQL clients/q'
	#TAIL_PID=$!
	#echo "tail pid ${TAIL_PID}"

	PID=$(<"$PID_FILE")

	echo "Cassandra ${v} running (PID ${PID}), about to run test suite ...."

	go test -v ./...

	echo "Test suite passed against Cassandra ${v}, killing server instance (PID ${PID})"
	
	kill -9 $PID
	rm $PID_FILE
done