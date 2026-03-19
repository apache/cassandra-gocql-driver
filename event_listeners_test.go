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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2012, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type mockKeyspaceChangeListener struct {
	createdCount int
	updatedCount int
	droppedCount int
}

func (m *mockKeyspaceChangeListener) OnKeyspaceCreated(_ OnKeyspaceCreatedEvent) { m.createdCount++ }
func (m *mockKeyspaceChangeListener) OnKeyspaceUpdated(_ OnKeyspaceUpdatedEvent) { m.updatedCount++ }
func (m *mockKeyspaceChangeListener) OnKeyspaceDropped(_ OnKeyspaceDroppedEvent) { m.droppedCount++ }

type mockTableChangeListener struct {
	createdCount int
	updatedCount int
	droppedCount int
}

func (m *mockTableChangeListener) OnTableCreated(_ OnTableCreatedEvent) { m.createdCount++ }
func (m *mockTableChangeListener) OnTableUpdated(_ OnTableUpdatedEvent) { m.updatedCount++ }
func (m *mockTableChangeListener) OnTableDropped(_ OnTableDroppedEvent) { m.droppedCount++ }

type mockUserTypeChangeListener struct {
	createdCount int
	updatedCount int
	droppedCount int
}

func (m *mockUserTypeChangeListener) OnUserTypeCreated(_ OnUserTypeCreatedEvent) { m.createdCount++ }
func (m *mockUserTypeChangeListener) OnUserTypeUpdated(_ OnUserTypeUpdatedEvent) { m.updatedCount++ }
func (m *mockUserTypeChangeListener) OnUserTypeDropped(_ OnUserTypeDroppedEvent) { m.droppedCount++ }

type mockFunctionChangeListener struct {
	createdCount int
	updatedCount int
	droppedCount int
}

func (m *mockFunctionChangeListener) OnFunctionCreated(_ OnFunctionCreatedEvent) { m.createdCount++ }
func (m *mockFunctionChangeListener) OnFunctionUpdated(_ OnFunctionUpdatedEvent) { m.updatedCount++ }
func (m *mockFunctionChangeListener) OnFunctionDropped(_ OnFunctionDroppedEvent) { m.droppedCount++ }

type mockAggregateChangeListener struct {
	createdCount int
	updatedCount int
	droppedCount int
}

func (m *mockAggregateChangeListener) OnAggregateCreated(_ OnAggregateCreatedEvent) {
	m.createdCount++
}
func (m *mockAggregateChangeListener) OnAggregateUpdated(_ OnAggregateUpdatedEvent) {
	m.updatedCount++
}
func (m *mockAggregateChangeListener) OnAggregateDropped(_ OnAggregateDroppedEvent) {
	m.droppedCount++
}

// Mock implementations for host state and topology change listener interfaces.

type mockHostStatusChangeListener struct {
	hostUpCount   int
	hostDownCount int
}

func (m *mockHostStatusChangeListener) OnHostUp(_ HostUpEvent)     { m.hostUpCount++ }
func (m *mockHostStatusChangeListener) OnHostDown(_ HostDownEvent) { m.hostDownCount++ }

type mockTopologyChangeListener struct {
	newHostCount     int
	removedHostCount int
}

func (m *mockTopologyChangeListener) OnNewHost(_ NewHostEvent)         { m.newHostCount++ }
func (m *mockTopologyChangeListener) OnRemovedHost(_ RemovedHostEvent) { m.removedHostCount++ }

func TestSchemaListenersMux_Keyspaces(t *testing.T) {
	t.Run("no listeners", func(t *testing.T) {
		mux := &SchemaListenersMux{}
		mux.OnKeyspaceCreated(OnKeyspaceCreatedEvent{Keyspace: &KeyspaceMetadata{Name: "ks"}})
		mux.OnKeyspaceUpdated(OnKeyspaceUpdatedEvent{
			Old: &KeyspaceMetadata{Name: "ks"},
			New: &KeyspaceMetadata{Name: "ks"},
		})
		mux.OnKeyspaceDropped(OnKeyspaceDroppedEvent{Keyspace: &KeyspaceMetadata{Name: "ks"}})
	})

	t.Run("single listener", func(t *testing.T) {
		l := &mockKeyspaceChangeListener{}
		mux := &SchemaListenersMux{
			Keyspaces: []KeyspaceChangeListener{l},
		}

		mux.OnKeyspaceCreated(OnKeyspaceCreatedEvent{Keyspace: &KeyspaceMetadata{Name: "ks"}})
		mux.OnKeyspaceUpdated(OnKeyspaceUpdatedEvent{
			Old: &KeyspaceMetadata{Name: "ks"},
			New: &KeyspaceMetadata{Name: "ks"},
		})
		mux.OnKeyspaceDropped(OnKeyspaceDroppedEvent{Keyspace: &KeyspaceMetadata{Name: "ks"}})

		assertCounts(t, "created", l.createdCount, 1)
		assertCounts(t, "updated", l.updatedCount, 1)
		assertCounts(t, "dropped", l.droppedCount, 1)
	})

	t.Run("multiple listeners", func(t *testing.T) {
		l1 := &mockKeyspaceChangeListener{}
		l2 := &mockKeyspaceChangeListener{}
		l3 := &mockKeyspaceChangeListener{}
		mux := &SchemaListenersMux{
			Keyspaces: []KeyspaceChangeListener{l1, l2, l3},
		}

		mux.OnKeyspaceCreated(OnKeyspaceCreatedEvent{Keyspace: &KeyspaceMetadata{Name: "ks"}})
		mux.OnKeyspaceUpdated(OnKeyspaceUpdatedEvent{
			Old: &KeyspaceMetadata{Name: "ks"},
			New: &KeyspaceMetadata{Name: "ks"},
		})
		mux.OnKeyspaceDropped(OnKeyspaceDroppedEvent{Keyspace: &KeyspaceMetadata{Name: "ks"}})

		for i, l := range []*mockKeyspaceChangeListener{l1, l2, l3} {
			assertCountsIdx(t, i, "created", l.createdCount, 1)
			assertCountsIdx(t, i, "updated", l.updatedCount, 1)
			assertCountsIdx(t, i, "dropped", l.droppedCount, 1)
		}
	})
}

func TestSchemaListenersMux_Tables(t *testing.T) {
	t.Run("no listeners", func(t *testing.T) {
		mux := &SchemaListenersMux{}
		mux.OnTableCreated(OnTableCreatedEvent{Table: &TableMetadata{Name: "tbl"}})
		mux.OnTableUpdated(OnTableUpdatedEvent{
			Old: &TableMetadata{Name: "tbl"},
			New: &TableMetadata{Name: "tbl"},
		})
		mux.OnTableDropped(OnTableDroppedEvent{Table: &TableMetadata{Name: "tbl"}})
	})

	t.Run("single listener", func(t *testing.T) {
		l := &mockTableChangeListener{}
		mux := &SchemaListenersMux{
			Tables: []TableChangeListener{l},
		}

		mux.OnTableCreated(OnTableCreatedEvent{Table: &TableMetadata{Name: "tbl"}})
		mux.OnTableUpdated(OnTableUpdatedEvent{
			Old: &TableMetadata{Name: "tbl"},
			New: &TableMetadata{Name: "tbl"},
		})
		mux.OnTableDropped(OnTableDroppedEvent{Table: &TableMetadata{Name: "tbl"}})

		assertCounts(t, "created", l.createdCount, 1)
		assertCounts(t, "updated", l.updatedCount, 1)
		assertCounts(t, "dropped", l.droppedCount, 1)
	})

	t.Run("multiple listeners", func(t *testing.T) {
		l1 := &mockTableChangeListener{}
		l2 := &mockTableChangeListener{}
		l3 := &mockTableChangeListener{}
		mux := &SchemaListenersMux{
			Tables: []TableChangeListener{l1, l2, l3},
		}

		mux.OnTableCreated(OnTableCreatedEvent{Table: &TableMetadata{Name: "tbl"}})
		mux.OnTableUpdated(OnTableUpdatedEvent{
			Old: &TableMetadata{Name: "tbl"},
			New: &TableMetadata{Name: "tbl"},
		})
		mux.OnTableDropped(OnTableDroppedEvent{Table: &TableMetadata{Name: "tbl"}})

		for i, l := range []*mockTableChangeListener{l1, l2, l3} {
			assertCountsIdx(t, i, "created", l.createdCount, 1)
			assertCountsIdx(t, i, "updated", l.updatedCount, 1)
			assertCountsIdx(t, i, "dropped", l.droppedCount, 1)
		}
	})
}

func TestSchemaListenersMux_UserTypes(t *testing.T) {
	t.Run("no listeners", func(t *testing.T) {
		mux := &SchemaListenersMux{}
		mux.OnUserTypeCreated(OnUserTypeCreatedEvent{UserType: &UserTypeMetadata{Name: "udt"}})
		mux.OnUserTypeUpdated(OnUserTypeUpdatedEvent{
			Old: &UserTypeMetadata{Name: "udt"},
			New: &UserTypeMetadata{Name: "udt"},
		})
		mux.OnUserTypeDropped(OnUserTypeDroppedEvent{UserType: &UserTypeMetadata{Name: "udt"}})
	})

	t.Run("single listener", func(t *testing.T) {
		l := &mockUserTypeChangeListener{}
		mux := &SchemaListenersMux{
			UserTypes: []UserTypeChangeListener{l},
		}

		mux.OnUserTypeCreated(OnUserTypeCreatedEvent{UserType: &UserTypeMetadata{Name: "udt"}})
		mux.OnUserTypeUpdated(OnUserTypeUpdatedEvent{
			Old: &UserTypeMetadata{Name: "udt"},
			New: &UserTypeMetadata{Name: "udt"},
		})
		mux.OnUserTypeDropped(OnUserTypeDroppedEvent{UserType: &UserTypeMetadata{Name: "udt"}})

		assertCounts(t, "created", l.createdCount, 1)
		assertCounts(t, "updated", l.updatedCount, 1)
		assertCounts(t, "dropped", l.droppedCount, 1)
	})

	t.Run("multiple listeners", func(t *testing.T) {
		l1 := &mockUserTypeChangeListener{}
		l2 := &mockUserTypeChangeListener{}
		l3 := &mockUserTypeChangeListener{}
		mux := &SchemaListenersMux{
			UserTypes: []UserTypeChangeListener{l1, l2, l3},
		}

		mux.OnUserTypeCreated(OnUserTypeCreatedEvent{UserType: &UserTypeMetadata{Name: "udt"}})
		mux.OnUserTypeUpdated(OnUserTypeUpdatedEvent{
			Old: &UserTypeMetadata{Name: "udt"},
			New: &UserTypeMetadata{Name: "udt"},
		})
		mux.OnUserTypeDropped(OnUserTypeDroppedEvent{UserType: &UserTypeMetadata{Name: "udt"}})

		for i, l := range []*mockUserTypeChangeListener{l1, l2, l3} {
			assertCountsIdx(t, i, "created", l.createdCount, 1)
			assertCountsIdx(t, i, "updated", l.updatedCount, 1)
			assertCountsIdx(t, i, "dropped", l.droppedCount, 1)
		}
	})
}

func TestSchemaListenersMux_Functions(t *testing.T) {
	t.Run("no listeners", func(t *testing.T) {
		mux := &SchemaListenersMux{}
		mux.OnFunctionCreated(OnFunctionCreatedEvent{Function: &FunctionMetadata{Name: "fn"}})
		mux.OnFunctionUpdated(OnFunctionUpdatedEvent{
			Old: &FunctionMetadata{Name: "fn"},
			New: &FunctionMetadata{Name: "fn"},
		})
		mux.OnFunctionDropped(OnFunctionDroppedEvent{Function: &FunctionMetadata{Name: "fn"}})
	})

	t.Run("single listener", func(t *testing.T) {
		l := &mockFunctionChangeListener{}
		mux := &SchemaListenersMux{
			Functions: []FunctionChangeListener{l},
		}

		mux.OnFunctionCreated(OnFunctionCreatedEvent{Function: &FunctionMetadata{Name: "fn"}})
		mux.OnFunctionUpdated(OnFunctionUpdatedEvent{
			Old: &FunctionMetadata{Name: "fn"},
			New: &FunctionMetadata{Name: "fn"},
		})
		mux.OnFunctionDropped(OnFunctionDroppedEvent{Function: &FunctionMetadata{Name: "fn"}})

		assertCounts(t, "created", l.createdCount, 1)
		assertCounts(t, "updated", l.updatedCount, 1)
		assertCounts(t, "dropped", l.droppedCount, 1)
	})

	t.Run("multiple listeners", func(t *testing.T) {
		l1 := &mockFunctionChangeListener{}
		l2 := &mockFunctionChangeListener{}
		l3 := &mockFunctionChangeListener{}
		mux := &SchemaListenersMux{
			Functions: []FunctionChangeListener{l1, l2, l3},
		}

		mux.OnFunctionCreated(OnFunctionCreatedEvent{Function: &FunctionMetadata{Name: "fn"}})
		mux.OnFunctionUpdated(OnFunctionUpdatedEvent{
			Old: &FunctionMetadata{Name: "fn"},
			New: &FunctionMetadata{Name: "fn"},
		})
		mux.OnFunctionDropped(OnFunctionDroppedEvent{Function: &FunctionMetadata{Name: "fn"}})

		for i, l := range []*mockFunctionChangeListener{l1, l2, l3} {
			assertCountsIdx(t, i, "created", l.createdCount, 1)
			assertCountsIdx(t, i, "updated", l.updatedCount, 1)
			assertCountsIdx(t, i, "dropped", l.droppedCount, 1)
		}
	})
}

func TestSchemaListenersMux_Aggregates(t *testing.T) {
	t.Run("no listeners", func(t *testing.T) {
		mux := &SchemaListenersMux{}
		mux.OnAggregateCreated(OnAggregateCreatedEvent{Aggregate: &AggregateMetadata{Name: "agg"}})
		mux.OnAggregateUpdated(OnAggregateUpdatedEvent{
			Old: &AggregateMetadata{Name: "agg"},
			New: &AggregateMetadata{Name: "agg"},
		})
		mux.OnAggregateDropped(OnAggregateDroppedEvent{Aggregate: &AggregateMetadata{Name: "agg"}})
	})

	t.Run("single listener", func(t *testing.T) {
		l := &mockAggregateChangeListener{}
		mux := &SchemaListenersMux{
			Aggregates: []AggregateChangeListener{l},
		}

		mux.OnAggregateCreated(OnAggregateCreatedEvent{Aggregate: &AggregateMetadata{Name: "agg"}})
		mux.OnAggregateUpdated(OnAggregateUpdatedEvent{
			Old: &AggregateMetadata{Name: "agg"},
			New: &AggregateMetadata{Name: "agg"},
		})
		mux.OnAggregateDropped(OnAggregateDroppedEvent{Aggregate: &AggregateMetadata{Name: "agg"}})

		assertCounts(t, "created", l.createdCount, 1)
		assertCounts(t, "updated", l.updatedCount, 1)
		assertCounts(t, "dropped", l.droppedCount, 1)
	})

	t.Run("multiple listeners", func(t *testing.T) {
		l1 := &mockAggregateChangeListener{}
		l2 := &mockAggregateChangeListener{}
		l3 := &mockAggregateChangeListener{}
		mux := &SchemaListenersMux{
			Aggregates: []AggregateChangeListener{l1, l2, l3},
		}

		mux.OnAggregateCreated(OnAggregateCreatedEvent{Aggregate: &AggregateMetadata{Name: "agg"}})
		mux.OnAggregateUpdated(OnAggregateUpdatedEvent{
			Old: &AggregateMetadata{Name: "agg"},
			New: &AggregateMetadata{Name: "agg"},
		})
		mux.OnAggregateDropped(OnAggregateDroppedEvent{Aggregate: &AggregateMetadata{Name: "agg"}})

		for i, l := range []*mockAggregateChangeListener{l1, l2, l3} {
			assertCountsIdx(t, i, "created", l.createdCount, 1)
			assertCountsIdx(t, i, "updated", l.updatedCount, 1)
			assertCountsIdx(t, i, "dropped", l.droppedCount, 1)
		}
	})
}

func TestSchemaListenersMux_OnlyTargetCategoryReceivesEvents(t *testing.T) {
	ksListener := &mockKeyspaceChangeListener{}
	tblListener := &mockTableChangeListener{}
	udtListener := &mockUserTypeChangeListener{}
	fnListener := &mockFunctionChangeListener{}
	aggListener := &mockAggregateChangeListener{}

	mux := &SchemaListenersMux{
		Keyspaces:  []KeyspaceChangeListener{ksListener},
		Tables:     []TableChangeListener{tblListener},
		UserTypes:  []UserTypeChangeListener{udtListener},
		Functions:  []FunctionChangeListener{fnListener},
		Aggregates: []AggregateChangeListener{aggListener},
	}

	mux.OnKeyspaceCreated(OnKeyspaceCreatedEvent{Keyspace: &KeyspaceMetadata{Name: "ks"}})

	assertCounts(t, "keyspace created", ksListener.createdCount, 1)
	assertCounts(t, "table created", tblListener.createdCount, 0)
	assertCounts(t, "usertype created", udtListener.createdCount, 0)
	assertCounts(t, "function created", fnListener.createdCount, 0)
	assertCounts(t, "aggregate created", aggListener.createdCount, 0)
}

func TestHostListenersMux_HostStatus(t *testing.T) {
	t.Run("no listeners", func(t *testing.T) {
		mux := HostListenersMux{}
		mux.OnHostUp(HostUpEvent{Host: &HostInfo{hostId: "h1"}})
		mux.OnHostDown(HostDownEvent{Host: &HostInfo{hostId: "h1"}})
	})

	t.Run("single listener", func(t *testing.T) {
		l := &mockHostStatusChangeListener{}
		mux := HostListenersMux{
			HostStateChangeListeners: []HostStatusChangeListener{l},
		}

		mux.OnHostUp(HostUpEvent{Host: &HostInfo{hostId: "h1"}})
		mux.OnHostDown(HostDownEvent{Host: &HostInfo{hostId: "h1"}})

		assertCounts(t, "host up", l.hostUpCount, 1)
		assertCounts(t, "host down", l.hostDownCount, 1)
	})

	t.Run("multiple listeners", func(t *testing.T) {
		l1 := &mockHostStatusChangeListener{}
		l2 := &mockHostStatusChangeListener{}
		l3 := &mockHostStatusChangeListener{}
		mux := HostListenersMux{
			HostStateChangeListeners: []HostStatusChangeListener{l1, l2, l3},
		}

		mux.OnHostUp(HostUpEvent{Host: &HostInfo{hostId: "h1"}})
		mux.OnHostDown(HostDownEvent{Host: &HostInfo{hostId: "h1"}})

		for i, l := range []*mockHostStatusChangeListener{l1, l2, l3} {
			assertCountsIdx(t, i, "host up", l.hostUpCount, 1)
			assertCountsIdx(t, i, "host down", l.hostDownCount, 1)
		}
	})
}

func TestHostListenersMux_Topology(t *testing.T) {
	t.Run("no listeners", func(t *testing.T) {
		mux := HostListenersMux{}
		mux.OnNewHost(NewHostEvent{Host: &HostInfo{hostId: "h1"}})
		mux.OnRemovedHost(RemovedHostEvent{Host: &HostInfo{hostId: "h1"}})
	})

	t.Run("single listener", func(t *testing.T) {
		l := &mockTopologyChangeListener{}
		mux := HostListenersMux{
			TopologyChangeListeners: []TopologyChangeListener{l},
		}

		mux.OnNewHost(NewHostEvent{Host: &HostInfo{hostId: "h1"}})
		mux.OnRemovedHost(RemovedHostEvent{Host: &HostInfo{hostId: "h1"}})

		assertCounts(t, "new host", l.newHostCount, 1)
		assertCounts(t, "removed host", l.removedHostCount, 1)
	})

	t.Run("multiple listeners", func(t *testing.T) {
		l1 := &mockTopologyChangeListener{}
		l2 := &mockTopologyChangeListener{}
		l3 := &mockTopologyChangeListener{}
		mux := HostListenersMux{
			TopologyChangeListeners: []TopologyChangeListener{l1, l2, l3},
		}

		mux.OnNewHost(NewHostEvent{Host: &HostInfo{hostId: "h1"}})
		mux.OnRemovedHost(RemovedHostEvent{Host: &HostInfo{hostId: "h1"}})

		for i, l := range []*mockTopologyChangeListener{l1, l2, l3} {
			assertCountsIdx(t, i, "new host", l.newHostCount, 1)
			assertCountsIdx(t, i, "removed host", l.removedHostCount, 1)
		}
	})
}

func TestHostListenersMux_OnlyTargetCategoryReceivesEvents(t *testing.T) {
	statusListener := &mockHostStatusChangeListener{}
	topoListener := &mockTopologyChangeListener{}

	mux := HostListenersMux{
		HostStateChangeListeners: []HostStatusChangeListener{statusListener},
		TopologyChangeListeners:  []TopologyChangeListener{topoListener},
	}

	mux.OnHostUp(HostUpEvent{Host: &HostInfo{hostId: "h1"}})

	assertCounts(t, "host up", statusListener.hostUpCount, 1)
	assertCounts(t, "host down", statusListener.hostDownCount, 0)
	assertCounts(t, "new host", topoListener.newHostCount, 0)
	assertCounts(t, "removed host", topoListener.removedHostCount, 0)
}

type mockSessionReadyListenerUnit struct {
	readyCount int
}

func (m *mockSessionReadyListenerUnit) OnSessionReady(_ *Session) { m.readyCount++ }

func TestSessionReadyListenersMux(t *testing.T) {
	t.Run("no listeners", func(t *testing.T) {
		mux := &SessionReadyListenersMux{}
		mux.OnSessionReady(nil)
	})

	t.Run("single listener", func(t *testing.T) {
		l := &mockSessionReadyListenerUnit{}
		mux := &SessionReadyListenersMux{
			SessionReady: []SessionReadyListener{l},
		}

		mux.OnSessionReady(nil)
	})

	t.Run("multiple listeners", func(t *testing.T) {
		l1 := &mockSessionReadyListenerUnit{}
		l2 := &mockSessionReadyListenerUnit{}
		l3 := &mockSessionReadyListenerUnit{}
		mux := &SessionReadyListenersMux{
			SessionReady: []SessionReadyListener{l1, l2, l3},
		}

		mux.OnSessionReady(nil)

		for i, l := range []*mockSessionReadyListenerUnit{l1, l2, l3} {
			assertCountsIdx(t, i, "ready", l.readyCount, 1)
		}
	})
}

func assertCounts(t *testing.T, label string, got, want int) {
	t.Helper()
	require.Equal(t, want, got, "%s: got %d, want %d", label, got, want)
}

func assertCountsIdx(t *testing.T, idx int, label string, got, want int) {
	t.Helper()
	require.Equal(t, want, got, "listener[%d] %s: got %d, want %d", idx, label, got, want)
}
