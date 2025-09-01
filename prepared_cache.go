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
	"github.com/maypok86/otter/v2"
)

const defaultMaxPreparedStmts = 1000

func NewPreparedLRU(size int) *otter.Cache[string, *preparedStatment] {
	return otter.Must(&otter.Options[string, *preparedStatment]{
		InitialCapacity: size,
		MaximumSize:     size,
	})
}

func keyForPreparedStatement(hostID, keyspace, statement string) string {
	// TODO: we should just use a struct for the key in the map
	return hostID + keyspace + statement
}
