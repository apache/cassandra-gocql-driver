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

package gocql_test

import (
	"fmt"
	"log"
	"sync"

	gocql "github.com/apache/cassandra-gocql-driver/v2"
)

// SchemaStateListener implements schema listener interfaces and SessionReadyListener
// to track and print the current state of schema metadata.
// This example demonstrates KeyspaceChangeListener and TableChangeListener.
// Additional listeners are available: UserTypeChangeListener, FunctionChangeListener, and AggregateChangeListener.
//
// Note: No mutex needed because schema events are serialized and the driver's sessionStateMu
// provides memory visibility guarantees between OnSessionReady and event callbacks.
type SchemaStateListener struct {
	keyspaces map[string]*gocql.KeyspaceMetadata
}

// NewSchemaStateListener creates a new schema state listener.
func NewSchemaStateListener() *SchemaStateListener {
	return &SchemaStateListener{
		keyspaces: make(map[string]*gocql.KeyspaceMetadata),
	}
}

// OnSessionReady is called when the session is ready and provides initial schema state.
func (l *SchemaStateListener) OnSessionReady(session *gocql.Session) {
	fmt.Println("Schema Listener: Session ready, loading initial schema state")

	// Get all keyspace metadata (includes tables, types, functions, etc.)
	// The driver returns a copy of the map, so we can store and modify it safely
	allKeyspaces, err := session.AllKeyspaceMetadata()
	if err != nil {
		log.Printf("Error getting all keyspace metadata: %v", err)
		return
	}

	l.keyspaces = allKeyspaces

	// Print loaded keyspaces
	for keyspaceName, ks := range l.keyspaces {
		fmt.Printf("  Initial keyspace: %s (replication: %v)\n", keyspaceName, ks.StrategyClass)
	}
}

// Keyspace change events
func (l *SchemaStateListener) OnKeyspaceCreated(event gocql.OnKeyspaceCreatedEvent) {
	l.keyspaces[event.Keyspace.Name] = event.Keyspace
	fmt.Printf("Schema Event: Keyspace created: %s\n", event.Keyspace.Name)
}

func (l *SchemaStateListener) OnKeyspaceUpdated(event gocql.OnKeyspaceUpdatedEvent) {
	l.keyspaces[event.New.Name] = event.New
	fmt.Printf("Schema Event: Keyspace updated: %s\n", event.New.Name)
}

func (l *SchemaStateListener) OnKeyspaceDropped(event gocql.OnKeyspaceDroppedEvent) {
	delete(l.keyspaces, event.Keyspace.Name)
	fmt.Printf("Schema Event: Keyspace dropped: %s\n", event.Keyspace.Name)
}

// Table change events
func (l *SchemaStateListener) OnTableCreated(event gocql.OnTableCreatedEvent) {
	// Update the keyspace metadata to include the new table
	if ks, ok := l.keyspaces[event.Table.Keyspace]; ok {
		if ks.Tables == nil {
			ks.Tables = make(map[string]*gocql.TableMetadata)
		}
		ks.Tables[event.Table.Name] = event.Table
	}
	fmt.Printf("Schema Event: Table created: %s.%s\n", event.Table.Keyspace, event.Table.Name)
}

func (l *SchemaStateListener) OnTableUpdated(event gocql.OnTableUpdatedEvent) {
	// Update the keyspace metadata with the new table version
	if ks, ok := l.keyspaces[event.New.Keyspace]; ok {
		if ks.Tables == nil {
			ks.Tables = make(map[string]*gocql.TableMetadata)
		}
		ks.Tables[event.New.Name] = event.New
	}
	fmt.Printf("Schema Event: Table updated: %s.%s\n", event.New.Keyspace, event.New.Name)
}

func (l *SchemaStateListener) OnTableDropped(event gocql.OnTableDroppedEvent) {
	// Remove the table from the keyspace metadata
	if ks, ok := l.keyspaces[event.Table.Keyspace]; ok && ks.Tables != nil {
		delete(ks.Tables, event.Table.Name)
	}
	fmt.Printf("Schema Event: Table dropped: %s.%s\n", event.Table.Keyspace, event.Table.Name)
}

// HostStateListener implements all host listener interfaces and SessionReadyListener
// to track and print the current state of cluster hosts.
//
// Thread Safety: This listener requires a mutex because:
//   - TopologyChangeListener callbacks (OnNewHost, OnRemovedHost) are called sequentially
//     from a single goroutine via the ring refresh mechanism
//   - HostStatusChangeListener callbacks (OnHostUp, OnHostDown) can be called concurrently
//     from multiple goroutines via the event debouncer
//   - Since this type implements BOTH interfaces, topology and status callbacks can run
//     at the same time from different event sources, requiring synchronization
//
// Important: If you only implement TopologyChangeListener OR only HostStatusChangeListener
// (but not both), you may not need a mutex. However, if you implement multiple listener
// interfaces (schema + host, or topology + status), you must use proper synchronization.
type HostStateListener struct {
	mu    sync.RWMutex
	hosts map[string]*gocql.HostInfo
}

// NewHostStateListener creates a new host state listener.
func NewHostStateListener() *HostStateListener {
	return &HostStateListener{
		hosts: make(map[string]*gocql.HostInfo),
	}
}

// OnSessionReady is called when the session is ready and provides initial host state.
func (l *HostStateListener) OnSessionReady(session *gocql.Session) {
	fmt.Println("Host Listener: Session ready, loading initial host state")

	// Get all hosts from the session
	hosts := session.GetHosts()

	for _, host := range hosts {
		l.hosts[host.HostID()] = host
		fmt.Printf("  Initial host: %s (id: %s, datacenter: %s, rack: %s, state: %s)\n",
			host.ConnectAddress(), host.HostID(), host.DataCenter(), host.Rack(), host.State())
	}
}

// Topology change events
func (l *HostStateListener) OnNewHost(event gocql.NewHostEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.hosts[event.Host.HostID()] = event.Host
	fmt.Printf("Host Event: New host added: %s (id: %s, datacenter: %s, rack: %s)\n",
		event.Host.ConnectAddress(), event.Host.HostID(), event.Host.DataCenter(), event.Host.Rack())
}

func (l *HostStateListener) OnRemovedHost(event gocql.RemovedHostEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.hosts, event.Host.HostID())
	fmt.Printf("Host Event: Host removed: %s (id: %s)\n",
		event.Host.ConnectAddress(), event.Host.HostID())
}

// Host state change events
func (l *HostStateListener) OnHostUp(event gocql.HostUpEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.hosts[event.Host.HostID()] = event.Host
	fmt.Printf("Host Event: Host up: %s (id: %s)\n",
		event.Host.ConnectAddress(), event.Host.HostID())
}

func (l *HostStateListener) OnHostDown(event gocql.HostDownEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Printf("Host Event: Host down: %s (id: %s)\n",
		event.Host.ConnectAddress(), event.Host.HostID())
}

// Example_eventListeners demonstrates how to implement and use SchemaListener and HostListener
// with SessionReadyListener to track the current state of schema and hosts in a Cassandra cluster.
//
// The example demonstrates KeyspaceChangeListener and TableChangeListener for schema events.
// Additional schema listeners are available if needed: UserTypeChangeListener, FunctionChangeListener,
// and AggregateChangeListener can be implemented following the same pattern.
//
// Note: If you need to register multiple listeners for the same event type, use the mux helper types
// (SchemaListenersMux, HostListenersMux, SessionReadyListenersMux).
func Example_eventListeners() {
	/* The example assumes the following CQL was used to setup the keyspace:
	create keyspace example with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
	create table example.events_demo(id int PRIMARY KEY, value text);
	*/

	// Create schema and host state listeners
	schemaListener := NewSchemaStateListener()
	hostListener := NewHostStateListener()

	// Configure the cluster with event listeners
	cluster := gocql.NewCluster("localhost:9042")
	cluster.Keyspace = "example"
	cluster.Consistency = gocql.LocalQuorum

	// Set up metadata configuration with listeners
	// Note: CacheMode defaults to Full, which is required for schema change events
	// Use SessionReadyListenersMux to notify both listeners when session is ready
	cluster.Metadata.SessionReadyListener = gocql.SessionReadyListenersMux{
		SessionReady: []gocql.SessionReadyListener{schemaListener, hostListener},
	}
	cluster.Metadata.SchemaListener = gocql.SchemaListenersConfig{
		KeyspaceChangeListener: schemaListener,
		TableChangeListener:    schemaListener,
		// UserTypeChangeListener, FunctionChangeListener, and AggregateChangeListener
		// can also be set here if needed
	}
	cluster.Metadata.HostListener = gocql.HostListenersConfig{
		HostStateChangeListener: hostListener,
		TopologyChangeListener:  hostListener,
	}

	// Create session - this will trigger OnSessionReady callbacks
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	fmt.Println("\nSession created successfully with event listeners configured")
	fmt.Println("Listeners will now receive events for schema and host changes")

	// Example output (actual output will vary based on your cluster):
	// Host Listener: Session ready, loading initial host state
	//   Initial host: 127.0.0.1 (id: a1b2c3d4-e5f6-7890-abcd-ef1234567890, datacenter: datacenter1, rack: rack1, state: UP)
	// Schema Listener: Session ready, loading initial schema state
	//   Initial keyspace: example (replication: org.apache.cassandra.locator.SimpleStrategy)
	//     Initial table: example.events_demo
	//   Initial keyspace: system (replication: org.apache.cassandra.locator.LocalStrategy)
	//     Initial table: system.local
	//     Initial table: system.peers
	//
	// Session created successfully with event listeners configured
	// Listeners will now receive events for schema and host changes
}
