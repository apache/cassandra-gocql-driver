//go:build all || cassandra
// +build all cassandra

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// This test tests that gocql is able to connect to a C* node provisioned with Docker
// This is useful to make sure we don't break common testing configurations
func TestDocker(t *testing.T) {
	version := "3.11.11"
	randomUuid := MustRandomUUID().String()
	err := exec.Command("docker", "run", "-d", "--name", randomUuid, "-p", "9080:9042", fmt.Sprintf("cassandra:%s", version)).Run()
	defer exec.Command("docker", "rm", "-f", randomUuid).Run()

	if err != nil {
		t.Fatal(err)
	}

	cluster := NewCluster("localhost:9080")
	cluster.Logger = NewLogger(LogLevelDebug)

	timer := time.After(60 * time.Second)
	var session *Session
	done := false
	for !done {
		select {
		case <-timer:
			t.Fatalf("timed out, last err: %v", err)
		default:
			session, err = cluster.CreateSession()
			if err == nil {
				done = true
			} else if strings.Contains(err.Error(), "unable to discover protocol version") {
				time.Sleep(5 * time.Second)
			} else {
				t.Fatal(err)
			}
		}
	}

	defer session.Close()
	var parsedVersion string
	err = session.Query("SELECT release_version FROM system.local").Scan(&parsedVersion)
	if err != nil {
		t.Fatalf("failed to query: %s", err)
	}

	assert.Equal(t, version, parsedVersion)
}
