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
	"net"
	"strconv"
	"sync"
)

type OneConnTestServer struct {
	Err  error
	Addr net.IP
	Port int

	listener   net.Listener
	acceptChan chan struct{}
	mu         sync.Mutex
	closed     bool
}

func NewOneConnTestServer() (*OneConnTestServer, error) {
	lstn, err := net.Listen("tcp4", "localhost:0")
	if err != nil {
		return nil, err
	}
	addr, port := parseAddressPort(lstn.Addr().String())
	return &OneConnTestServer{
		listener:   lstn,
		acceptChan: make(chan struct{}),
		Addr:       addr,
		Port:       port,
	}, nil
}

func (c *OneConnTestServer) Accepted() chan struct{} {
	return c.acceptChan
}

func (c *OneConnTestServer) Close() {
	c.lockedClose()
}

func (c *OneConnTestServer) Serve() {
	conn, err := c.listener.Accept()
	c.Err = err
	if conn != nil {
		conn.Close()
	}
	c.lockedClose()
}

func (c *OneConnTestServer) lockedClose() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		close(c.acceptChan)
		c.listener.Close()
		c.closed = true
	}
}

func parseAddressPort(hostPort string) (net.IP, int) {
	host, portStr, err := net.SplitHostPort(hostPort)
	if err != nil {
		return net.ParseIP(""), 0
	}
	port, _ := strconv.Atoi(portStr)
	return net.ParseIP(host), port
}
