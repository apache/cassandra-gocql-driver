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

package gocqlzap

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/gocql/gocql"
)

const logLineEnding = "%%%\n%%%"

func NewCustomLogger(pipeTo io.Writer) zapcore.Core {
	cfg := zap.NewProductionEncoderConfig()
	cfg.LineEnding = logLineEnding
	return zapcore.NewCore(
		zapcore.NewConsoleEncoder(cfg),
		zapcore.AddSync(pipeTo),
		zapcore.DebugLevel,
	)
}

func TestGocqlZapLog(t *testing.T) {
	b := &bytes.Buffer{}
	logger := zap.New(NewCustomLogger(b))
	clusterCfg := gocql.NewCluster("0.0.0.1")
	clusterCfg.Logger = NewZapLogger(logger)
	clusterCfg.ProtoVersion = 4
	session, err := clusterCfg.CreateSession()
	if err == nil {
		session.Close()
		t.Fatal("expected error creating session")
	}
	err = logger.Sync()
	if err != nil {
		t.Fatal("logger sync failed")
	}
	logOutput := strings.Split(b.String(), logLineEnding)
	found := false
	for _, logEntry := range logOutput {
		if len(logEntry) == 0 {
			continue
		}
		if !strings.Contains(logEntry, "info\tgocql\tControl connection failed to establish a connection to host.\t{\"host_addr\": "+
			"\"0.0.0.1\", \"port\": 9042, \"host_id\": \"\", \"err\": \"dial tcp 0.0.0.1:9042:") {
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
