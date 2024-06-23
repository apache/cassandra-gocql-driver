//go:build ccm
// +build ccm

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

package ccm

import (
	"testing"
)

func TestCCM(t *testing.T) {
	if err := AllUp(); err != nil {
		t.Fatal(err)
	}

	status, err := Status()
	if err != nil {
		t.Fatal(err)
	}

	if host, ok := status["node1"]; !ok {
		t.Fatal("node1 not in status list")
	} else if !host.State.IsUp() {
		t.Fatal("node1 is not up")
	}

	NodeDown("node1")
	status, err = Status()
	if err != nil {
		t.Fatal(err)
	}

	if host, ok := status["node1"]; !ok {
		t.Fatal("node1 not in status list")
	} else if host.State.IsUp() {
		t.Fatal("node1 is not down")
	}

	NodeUp("node1")
	status, err = Status()
	if err != nil {
		t.Fatal(err)
	}

	if host, ok := status["node1"]; !ok {
		t.Fatal("node1 not in status list")
	} else if !host.State.IsUp() {
		t.Fatal("node1 is not up")
	}
}
