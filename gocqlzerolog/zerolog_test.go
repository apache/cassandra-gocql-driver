//go:build all || unit
// +build all unit

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

package gocqlzerolog

import (
	"bytes"
	"strings"
	"testing"

	"github.com/rs/zerolog"

	"github.com/gocql/gocql"
)

const logLineEnding = "%%%\n%%%"

func TestGocqlZeroLog(t *testing.T) {
	b := &bytes.Buffer{}
	output := zerolog.ConsoleWriter{Out: b}
	output.NoColor = true
	output.FormatExtra = func(m map[string]interface{}, buffer *bytes.Buffer) error {
		buffer.WriteString(logLineEnding)
		return nil
	}
	logger := zerolog.New(output).Level(zerolog.DebugLevel)
	clusterCfg := gocql.NewCluster("0.0.0.1")
	clusterCfg.Logger = NewZerologLogger(logger)
	clusterCfg.ProtoVersion = 4
	session, err := clusterCfg.CreateSession()
	if err == nil {
		session.Close()
		t.Fatal("expected error creating session")
	}
	logOutput := strings.Split(b.String(), logLineEnding+"\n")
	found := false
	for _, logEntry := range logOutput {
		if len(logEntry) == 0 {
			continue
		}
		if !strings.Contains(logEntry, "Control connection failed to establish a connection to host.") ||
			!strings.Contains(logEntry, "host_addr=0.0.0.1 host_id= logger=gocql port=9042") {
			continue
		} else {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("log output didn't match expectations: ", strings.Join(logOutput, "\n"))
	}
}
