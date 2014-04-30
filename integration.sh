#!/bin/bash

set -e

PID_FILE=cassandra.pid
STARTUP_LOG=startup.log
CASSANDRA_LOG=/var/log/cassandra/system.log

for v in 2.0.7
do
	TARBALL=apache-cassandra-$v-bin.tar.gz
	CASSANDRA_DIR=apache-cassandra-$v

	curl -L -O -C - ftp://ftp.mirrorservice.org/sites/ftp.apache.org/cassandra/$v/$TARBALL
	
	if [ ! -f $CASSANDRA_DIR/bin/cassandra ]
	then
   		tar xzf $TARBALL
	fi

	echo "Booting Cassandra ${v}"

	#: >$STARTUP_LOG  # create an empty log file
	apache-cassandra-2.0.7/bin/cassandra -p $PID_FILE &> $STARTUP_LOG
	#while ! grep -q 'state jump to normal' $STARTUP_LOG; do sleep 1; done
	{ tail -n +1 -f $CASSANDRA_LOG & } | sed -n '/Starting listening for CQL clients/q'

	PID=$(<"$PID_FILE")

	echo "Cassandra ${v} running (PID ${PID})"

	go test -v ./...

	echo "Killing Cassandra ${v} (PID ${PID})"
	
	kill -9 $PID
	rm $PID_FILE
done