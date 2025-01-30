// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/gocql/gocql/internal"
)

type hostToken struct {
	token internal.Token
	host  *HostInfo
}

func (ht hostToken) String() string {
	return fmt.Sprintf("{token=%v host=%v}", ht.token, ht.host.HostID())
}

// a data structure for organizing the relationship between tokens and hosts
type tokenRing struct {
	partitioner internal.Partitioner

	// tokens map token range to primary replica.
	// The elements in tokens are sorted by token ascending.
	// The range for a given item in tokens starts after preceding range and ends with the token specified in
	// token. The end token is part of the range.
	// The lowest (i.e. index 0) range wraps around the ring (its preceding range is the one with largest index).
	tokens []hostToken

	hosts []*HostInfo
}

func newTokenRing(partitioner string, hosts []*HostInfo) (*tokenRing, error) {
	tokenRing := &tokenRing{
		hosts: hosts,
	}

	if strings.HasSuffix(partitioner, "Murmur3Partitioner") {
		tokenRing.partitioner = internal.Murmur3Partitioner{}
	} else if strings.HasSuffix(partitioner, "OrderedPartitioner") {
		tokenRing.partitioner = internal.OrderedPartitioner{}
	} else if strings.HasSuffix(partitioner, "RandomPartitioner") {
		tokenRing.partitioner = internal.RandomPartitioner{}
	} else {
		return nil, fmt.Errorf("unsupported partitioner '%s'", partitioner)
	}

	for _, host := range hosts {
		for _, strToken := range host.Tokens() {
			token := tokenRing.partitioner.ParseString(strToken)
			tokenRing.tokens = append(tokenRing.tokens, hostToken{token, host})
		}
	}

	sort.Sort(tokenRing)

	return tokenRing, nil
}

func (t *tokenRing) Len() int {
	return len(t.tokens)
}

func (t *tokenRing) Less(i, j int) bool {
	return t.tokens[i].token.Less(t.tokens[j].token)
}

func (t *tokenRing) Swap(i, j int) {
	t.tokens[i], t.tokens[j] = t.tokens[j], t.tokens[i]
}

func (t *tokenRing) String() string {
	buf := &bytes.Buffer{}
	buf.WriteString("TokenRing(")
	if t.partitioner != nil {
		buf.WriteString(t.partitioner.Name())
	}
	buf.WriteString("){")
	sep := ""
	for i, th := range t.tokens {
		buf.WriteString(sep)
		sep = ","
		buf.WriteString("\n\t[")
		buf.WriteString(strconv.Itoa(i))
		buf.WriteString("]")
		buf.WriteString(th.token.String())
		buf.WriteString(":")
		buf.WriteString(th.host.ConnectAddress().String())
	}
	buf.WriteString("\n}")
	return string(buf.Bytes())
}

func (t *tokenRing) GetHostForToken(token internal.Token) (host *HostInfo, endToken internal.Token) {
	if t == nil || len(t.tokens) == 0 {
		return nil, nil
	}

	// find the primary replica
	p := sort.Search(len(t.tokens), func(i int) bool {
		return !t.tokens[i].token.Less(token)
	})

	if p == len(t.tokens) {
		// wrap around to the first in the ring
		p = 0
	}

	v := t.tokens[p]
	return v.host, v.token
}
