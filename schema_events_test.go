//go:build cassandra
// +build cassandra

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

package gocql

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type schemaChangesTestListener struct {
	KeyspaceCreatedEvents []OnKeyspaceCreatedEvent
	KeyspaceUpdatedEvents []OnKeyspaceUpdatedEvent
	KeyspaceDroppedEvents []OnKeyspaceDroppedEvent

	TableCreatedEvents []OnTableCreatedEvent
	TableUpdatedEvents []OnTableUpdatedEvent
	TableDroppedEvents []OnTableDroppedEvent

	FunctionCreatedEvents []OnFunctionCreatedEvent
	FunctionUpdatedEvents []OnFunctionUpdatedEvent
	FunctionDroppedEvents []OnFunctionDroppedEvent

	AggregateCreatedEvents []OnAggregateCreatedEvent
	AggregateUpdatedEvents []OnAggregateUpdatedEvent
	AggregateDroppedEvents []OnAggregateDroppedEvent

	TypeCreatedEvents []OnUserTypeCreatedEvent
	TypeUpdatedEvents []OnUserTypeUpdatedEvent
	TypeDroppedEvents []OnUserTypeDroppedEvent
}

// OnAggregateCreated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) OnAggregateCreated(event OnAggregateCreatedEvent) {
	(*s).AggregateCreatedEvents = append(s.AggregateCreatedEvents, event)
}

// OnAggregateDropped implements [SchemaChangeListener].
func (s *schemaChangesTestListener) OnAggregateDropped(event OnAggregateDroppedEvent) {
	(*s).AggregateDroppedEvents = append(s.AggregateDroppedEvents, event)
}

// OnAggregateUpdated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) OnAggregateUpdated(event OnAggregateUpdatedEvent) {
	(*s).AggregateUpdatedEvents = append(s.AggregateUpdatedEvents, event)
}

// OnFunctionCreated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) OnFunctionCreated(event OnFunctionCreatedEvent) {
	(*s).FunctionCreatedEvents = append(s.FunctionCreatedEvents, event)
}

// OnFunctionDropped implements [SchemaChangeListener].
func (s *schemaChangesTestListener) OnFunctionDropped(event OnFunctionDroppedEvent) {
	(*s).FunctionDroppedEvents = append(s.FunctionDroppedEvents, event)
}

// OnFunctionUpdated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) OnFunctionUpdated(event OnFunctionUpdatedEvent) {
	(*s).FunctionUpdatedEvents = append(s.FunctionUpdatedEvents, event)
}

// OnKeyspaceCreated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) OnKeyspaceCreated(event OnKeyspaceCreatedEvent) {
	(*s).KeyspaceCreatedEvents = append(s.KeyspaceCreatedEvents, event)
}

// OnKeyspaceDropped implements [SchemaChangeListener].
func (s *schemaChangesTestListener) OnKeyspaceDropped(event OnKeyspaceDroppedEvent) {
	(*s).KeyspaceDroppedEvents = append(s.KeyspaceDroppedEvents, event)
}

// OnKeyspaceUpdated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) OnKeyspaceUpdated(event OnKeyspaceUpdatedEvent) {
	(*s).KeyspaceUpdatedEvents = append(s.KeyspaceUpdatedEvents, event)
}

// OnTableCreated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) OnTableCreated(event OnTableCreatedEvent) {
	(*s).TableCreatedEvents = append(s.TableCreatedEvents, event)
}

// OnTableDropped implements [SchemaChangeListener].
func (s *schemaChangesTestListener) OnTableDropped(event OnTableDroppedEvent) {
	(*s).TableDroppedEvents = append(s.TableDroppedEvents, event)
}

// OnTableUpdated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) OnTableUpdated(event OnTableUpdatedEvent) {
	(*s).TableUpdatedEvents = append(s.TableUpdatedEvents, event)
}

// OnTypeCreated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) OnUserTypeCreated(event OnUserTypeCreatedEvent) {
	(*s).TypeCreatedEvents = append(s.TypeCreatedEvents, event)
}

// OnTypeDropped implements [SchemaChangeListener].
func (s *schemaChangesTestListener) OnUserTypeDropped(event OnUserTypeDroppedEvent) {
	(*s).TypeDroppedEvents = append(s.TypeDroppedEvents, event)
}

// OnTypeUpdated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) OnUserTypeUpdated(event OnUserTypeUpdatedEvent) {
	(*s).TypeUpdatedEvents = append(s.TypeUpdatedEvents, event)
}

func (s *schemaChangesTestListener) clear() {
	s.KeyspaceCreatedEvents = nil
	s.KeyspaceDroppedEvents = nil
	s.KeyspaceUpdatedEvents = nil

	s.TableCreatedEvents = nil
	s.TableUpdatedEvents = nil
	s.TableDroppedEvents = nil

	s.FunctionCreatedEvents = nil
	s.FunctionUpdatedEvents = nil
	s.FunctionDroppedEvents = nil

	s.AggregateCreatedEvents = nil
	s.AggregateUpdatedEvents = nil
	s.AggregateDroppedEvents = nil

	s.TypeCreatedEvents = nil
	s.TypeUpdatedEvents = nil
	s.TypeDroppedEvents = nil
}

func TestSchemaEvents(t *testing.T) {
	listener := &schemaChangesTestListener{}

	session := createSession(t, func(config *ClusterConfig) {
		config.Metadata.SchemaListener = SchemaListenersConfig{
			KeyspaceChangeListener:  listener,
			TableChangeListener:     listener,
			UserTypeChangeListener:  listener,
			FunctionChangeListener:  listener,
			AggregateChangeListener: listener,
		}
		config.Events.DisableSchemaEvents = false
	})
	defer session.Close()

	// Verify that listeners are not called during session initialization.
	// During init, existing keyspaces/tables are detected as "new" in the metadata diff,
	// but the listener must not be notified because the session is not yet initialized.
	t.Run("no-events-during-initialization", func(t *testing.T) {
		require.Empty(t, listener.KeyspaceCreatedEvents)
		require.Empty(t, listener.KeyspaceUpdatedEvents)
		require.Empty(t, listener.KeyspaceDroppedEvents)
		require.Empty(t, listener.TableCreatedEvents)
		require.Empty(t, listener.TableUpdatedEvents)
		require.Empty(t, listener.TableDroppedEvents)
		require.Empty(t, listener.FunctionCreatedEvents)
		require.Empty(t, listener.FunctionUpdatedEvents)
		require.Empty(t, listener.FunctionDroppedEvents)
		require.Empty(t, listener.AggregateCreatedEvents)
		require.Empty(t, listener.AggregateUpdatedEvents)
		require.Empty(t, listener.AggregateDroppedEvents)
		require.Empty(t, listener.TypeCreatedEvents)
		require.Empty(t, listener.TypeUpdatedEvents)
		require.Empty(t, listener.TypeDroppedEvents)
	})

	t.Run("keyspace", func(t *testing.T) {
		testSchemaEventsKeyspace(t, session, listener)
	})

	t.Run("table", func(t *testing.T) {
		testSchemaEventsTable(t, session, listener)
	})

	t.Run("user-defined-function", func(t *testing.T) {
		testSchemaEventsFunction(t, session, listener)
	})

	t.Run("user-defined-aggregate", func(t *testing.T) {
		testSchemaEventsAggregate(t, session, listener)
	})

	t.Run("user-defined-type", func(t *testing.T) {
		testSchemaEventsType(t, session, listener)
	})
}

func testSchemaEventsKeyspace(t *testing.T, session *Session, listener *schemaChangesTestListener) {
	ks := randomNameWithPrefix("gocql_keyspace_events_")

	listener.clear()

	err := session.Query(fmt.Sprintf(`CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}`, ks)).Exec()
	require.NoError(t, err, "Expected no error creating keyspace")

	require.Eventually(t, func() bool {
		return len(listener.KeyspaceCreatedEvents) > 0
	}, time.Second*5, time.Millisecond*100, "Expected keyspace created event to be received")

	// Verify that the listener received the keyspace created event
	require.Equal(t, ks, listener.KeyspaceCreatedEvents[0].Keyspace.Name, "Expected keyspace created event to have correct keyspace name")
	require.Contains(t, listener.KeyspaceCreatedEvents[0].Keyspace.StrategyClass, "SimpleStrategy", "Expected keyspace created event to have correct replication class")
	require.Equal(t, "1", listener.KeyspaceCreatedEvents[0].Keyspace.StrategyOptions["replication_factor"], "Expected keyspace created event to have correct replication factor")

	listener.clear()

	err = session.Query(fmt.Sprintf(`ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}`, ks)).Exec()
	require.NoError(t, err, "Expected no error updating keyspace")

	// Verify that the listener received the keyspace updated event
	// and that the old and new metadata are correct

	require.Eventually(t, func() bool {
		return len(listener.KeyspaceUpdatedEvents) > 0
	}, time.Second*5, time.Millisecond*100, "Expected keyspace updated event to be received")

	// Old metadata
	require.Equal(t, ks, listener.KeyspaceUpdatedEvents[0].Old.Name, "Expected keyspace updated event to have correct keyspace name")
	require.Contains(t, listener.KeyspaceUpdatedEvents[0].Old.StrategyClass, "SimpleStrategy", "Expected keyspace created event to have correct replication class")
	require.Equal(t, "1", listener.KeyspaceUpdatedEvents[0].Old.StrategyOptions["replication_factor"], "Expected keyspace updated event to have correct old replication factor")

	// New metadata
	require.Equal(t, ks, listener.KeyspaceUpdatedEvents[0].New.Name, "Expected keyspace updated event to have correct keyspace name")
	require.Contains(t, listener.KeyspaceUpdatedEvents[0].New.StrategyClass, "SimpleStrategy", "Expected keyspace updated event to have correct replication class")
	require.Equal(t, "2", listener.KeyspaceUpdatedEvents[0].New.StrategyOptions["replication_factor"], "Expected keyspace updated event to have correct new replication factor")

	time.Sleep(2 * time.Second)

	listener.clear()

	err = session.Query(fmt.Sprintf(`DROP KEYSPACE %s`, ks)).
		RetryPolicy(&SimpleRetryPolicy{}).
		Consistency(All).
		Exec()
	require.NoError(t, err, "Expected no error dropping keyspace")

	require.Eventually(t, func() bool {
		return len(listener.KeyspaceDroppedEvents) > 0
	}, time.Second*60, time.Millisecond*100, "Expected keyspace dropped event to be received")

	// Verify that the listener received the keyspace dropped event
	require.Equal(t, ks, listener.KeyspaceDroppedEvents[0].Keyspace.Name, "Expected keyspace dropped event to have correct keyspace name")
}

func randomNameWithPrefix(prefix string) string {
	return prefix + strings.ToLower(randomText(10))
}

func testSchemaEventsTable(t *testing.T, session *Session, listener *schemaChangesTestListener) {
	table := randomNameWithPrefix("gocql_table_events_")

	listener.clear()

	err := session.Query(fmt.Sprintf("CREATE TABLE %s (id UUID PRIMARY KEY)", table)).Exec()
	require.NoError(t, err, "Expected no error creating table")

	require.Eventually(t, func() bool {
		return len(listener.TableCreatedEvents) > 0
	}, time.Second*5, time.Millisecond*100, "Expected table created event to be received")

	// Verify that the listener received the table created event
	require.Equal(t, table, listener.TableCreatedEvents[0].Table.Name, "Expected table created event to have correct table name")
	require.Contains(t, listener.TableCreatedEvents[0].Table.Columns, "id")
	require.IsType(t, listener.TableCreatedEvents[0].Table.Columns["id"].Type, uuidType{})

	listener.clear()

	err = session.Query(fmt.Sprintf("ALTER TABLE %s ADD name text", table)).Exec()
	require.NoError(t, err, "Expected no error updating table")

	// Verify that the listener received the table updated event
	// and that the old and new metadata are correct

	require.Eventually(t, func() bool {
		return len(listener.TableUpdatedEvents) > 0
	}, time.Second*5, time.Millisecond*100, "Expected table updated event to be received")

	// Old metadata
	require.Equal(t, table, listener.TableUpdatedEvents[0].Old.Name, "Expected table updated event to have correct table name")
	require.NotContains(t, listener.TableUpdatedEvents[0].Old.Columns, "name")

	// New metadata
	require.Equal(t, table, listener.TableUpdatedEvents[0].New.Name, "Expected table updated event to have correct table name")
	require.Contains(t, listener.TableUpdatedEvents[0].New.Columns, "name")

	err = session.Query(fmt.Sprintf("DROP TABLE %s", table)).Exec()
	require.NoError(t, err, "Expected no error dropping table")

	require.Eventually(t, func() bool {
		return len(listener.TableDroppedEvents) > 0
	}, time.Second*5, time.Millisecond*100, "Expected table dropped event to be received")

	// Verify that the listener received the table dropped event
	require.Equal(t, table, listener.TableDroppedEvents[0].Table.Name, "Expected table dropped event to have correct table name")
	require.Contains(t, listener.TableDroppedEvents[0].Table.Columns, "name")
}

func testSchemaEventsFunction(t *testing.T, session *Session, listener *schemaChangesTestListener) {
	functionName := randomNameWithPrefix("gocql_function_events_")
	functionBody1 := "return Integer.valueOf(input + 1);"

	listener.clear()

	err := session.Query(fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s (input int)
			RETURNS NULL ON NULL INPUT
			RETURNS int
			LANGUAGE java AS '%s'`, functionName, functionBody1)).Exec()
	require.NoError(t, err, "Expected no error creating function")
	require.Eventually(t, func() bool {
		return len(listener.FunctionCreatedEvents) > 0
	}, time.Second*5, time.Millisecond*100, "Expected function created event to be received")

	// Verify that the listener received the function created event
	require.Equal(t, functionName, listener.FunctionCreatedEvents[0].Function.Name, "Expected function created event to have correct function name")

	listener.clear()

	functionBody2 := "return Integer.valueOf(input + 2);"
	err = session.Query(fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s (input int)
			RETURNS NULL ON NULL INPUT
			RETURNS int
			LANGUAGE java AS '%s'`, functionName, functionBody2)).Exec()
	require.NoError(t, err, "Expected no error creating function")
	require.Eventually(t, func() bool {
		return len(listener.FunctionUpdatedEvents) > 0
	}, time.Second*5, time.Millisecond*100, "Expected function updated event to be received")

	require.Equal(t, functionName, listener.FunctionUpdatedEvents[0].Old.Name)
	require.Equal(t, functionName, listener.FunctionUpdatedEvents[0].New.Name)

	listener.clear()

	err = session.Query(fmt.Sprintf(`DROP FUNCTION %s (int)`, functionName)).Exec()
	require.NoError(t, err, "Expected no error dropping function")
	require.Eventually(t, func() bool {
		return len(listener.FunctionDroppedEvents) > 0
	}, time.Second*5, time.Millisecond*100, "Expected function dropped event to be received")

	require.Equal(t, functionName, listener.FunctionDroppedEvents[0].Function.Name)

}

func testSchemaEventsAggregate(t *testing.T, session *Session, listener *schemaChangesTestListener) {
	stateFuncName := randomNameWithPrefix("gocql_agg_state_")
	finalFuncName := randomNameWithPrefix("gocql_agg_final_")
	aggName := randomNameWithPrefix("gocql_aggregate_events_")

	listener.clear()

	err := session.Query(fmt.Sprintf(`CREATE OR REPLACE FUNCTION gocql_test.%s ( state tuple<int,bigint>, val int )
			CALLED ON NULL INPUT
			RETURNS tuple<int,bigint>
			LANGUAGE java AS
			$$if (val !=null) {state.setInt(0, state.getInt(0)+1); state.setLong(1, state.getLong(1)+val.intValue());}return state;$$;`, stateFuncName)).Exec()
	require.NoError(t, err, "Expected no error creating state function")

	err = session.Query(fmt.Sprintf(`CREATE OR REPLACE FUNCTION gocql_test.%s ( state tuple<int,bigint> )
			CALLED ON NULL INPUT
			RETURNS double
			LANGUAGE java AS
			$$double r = 0; if (state.getInt(0) == 0) return null; r = state.getLong(1); r/= state.getInt(0); return Double.valueOf(r);$$`, finalFuncName)).Exec()
	require.NoError(t, err, "Expected no error creating final function")

	err = session.Query(fmt.Sprintf(`CREATE OR REPLACE AGGREGATE gocql_test.%s (int)
			SFUNC %s
			STYPE tuple<int,bigint>
			FINALFUNC %s
			INITCOND (0,0);`, aggName, stateFuncName, finalFuncName)).Exec()
	require.NoError(t, err, "Expected no error creating aggregate")

	require.Eventually(t, func() bool {
		return len(listener.AggregateCreatedEvents) > 0
	}, time.Second*5, time.Millisecond*100, "Expected aggregate created event to be received")

	require.Equal(t, aggName, listener.AggregateCreatedEvents[0].Aggregate.Name, "Expected aggregate created event to have correct aggregate name")

	listener.clear()

	err = session.Query(fmt.Sprintf(`CREATE OR REPLACE AGGREGATE gocql_test.%s (int)
			SFUNC %s
			STYPE tuple<int,bigint>
			FINALFUNC %s
			INITCOND (1,1);`, aggName, stateFuncName, finalFuncName)).Exec()
	require.NoError(t, err, "Expected no error updating aggregate")

	require.Eventually(t, func() bool {
		return len(listener.AggregateUpdatedEvents) > 0
	}, time.Second*5, time.Millisecond*100, "Expected aggregate updated event to be received")

	require.Equal(t, aggName, listener.AggregateUpdatedEvents[0].Old.Name, "Expected aggregate updated event to have correct old aggregate name")
	require.Equal(t, aggName, listener.AggregateUpdatedEvents[0].New.Name, "Expected aggregate updated event to have correct new aggregate name")

	listener.clear()

	err = session.Query(fmt.Sprintf(`DROP AGGREGATE gocql_test.%s (int)`, aggName)).Exec()
	require.NoError(t, err, "Expected no error dropping aggregate")

	require.Eventually(t, func() bool {
		return len(listener.AggregateDroppedEvents) > 0
	}, time.Second*5, time.Millisecond*100, "Expected aggregate dropped event to be received")

	require.Equal(t, aggName, listener.AggregateDroppedEvents[0].Aggregate.Name, "Expected aggregate dropped event to have correct aggregate name")
}

func testSchemaEventsType(t *testing.T, session *Session, listener *schemaChangesTestListener) {
	typeName := randomNameWithPrefix("gocql_type_events_")

	listener.clear()

	err := session.Query(fmt.Sprintf(`CREATE TYPE %s (id int, name text)`, typeName)).Exec()
	require.NoError(t, err, "Expected no error creating type")

	require.Eventually(t, func() bool {
		return len(listener.TypeCreatedEvents) > 0
	}, time.Second*5, time.Millisecond*100, "Expected type created event to be received")

	require.Equal(t, typeName, listener.TypeCreatedEvents[0].UserType.Name)
	require.Contains(t, listener.TypeCreatedEvents[0].UserType.FieldNames, "id")
	require.Contains(t, listener.TypeCreatedEvents[0].UserType.FieldNames, "name")

	listener.clear()

	err = session.Query(fmt.Sprintf(`ALTER TYPE %s ADD age int`, typeName)).Exec()
	require.NoError(t, err, "Expected no error updating type")

	require.Eventually(t, func() bool {
		return len(listener.TypeUpdatedEvents) > 0
	}, time.Second*5, time.Millisecond*100, "Expected type updated event to be received")

	require.Equal(t, typeName, listener.TypeUpdatedEvents[0].Old.Name)
	require.NotContains(t, listener.TypeUpdatedEvents[0].Old.FieldNames, "age")

	require.Equal(t, typeName, listener.TypeUpdatedEvents[0].New.Name)
	require.Contains(t, listener.TypeUpdatedEvents[0].New.FieldNames, "age")

	err = session.Query(fmt.Sprintf(`DROP TYPE %s`, typeName)).Exec()
	require.NoError(t, err, "Expected no error dropping type")

	require.Eventually(t, func() bool {
		return len(listener.TypeDroppedEvents) > 0
	}, time.Second*5, time.Millisecond*100, "Expected type dropped event to be received")

	require.Equal(t, typeName, listener.TypeDroppedEvents[0].UserType.Name)
}
