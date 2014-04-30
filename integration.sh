#!/bin/bash

PID_FILE=cassandra.pid

for v in 2.0.7
do
	TARBALL=apache-cassandra-$v-bin.tar.gz
	CASSANDRA_DIR=apache-cassandra-$v
	STARTUP_LOG=startup.log

	curl -L -O -C - ftp://ftp.mirrorservice.org/sites/ftp.apache.org/cassandra/$v/$TARBALL
	
	if [ ! -f $CASSANDRA_DIR/bin/cassandra ]
	then
   		tar xzvf $TARBALL
	fi

	echo "Booting Cassandra ${v}"

	apache-cassandra-2.0.7/bin/cassandra -p $PID_FILE &> $STARTUP_LOG
	while ! grep -q 'state jump to normal' $STARTUP_LOG; do sleep 1; done

	read PID <$PID_FILE

	echo "Cassandra ${v} running (PID ${PID})"

	go test -v ./...

	echo "Killing Cassandra ${v} (PID ${PID})"
	
	kill -9 $PID
	rm $PID_FILE
done