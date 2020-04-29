// +build !cassandra scylla

// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"fmt"
	"strings"
	"sync"
)

// schema metadata for a keyspace
type KeyspaceMetadata struct {
	Name            string
	DurableWrites   bool
	StrategyClass   string
	StrategyOptions map[string]interface{}
	Tables          map[string]*TableMetadata
	Functions       map[string]*FunctionMetadata
	Aggregates      map[string]*AggregateMetadata
	Types           map[string]*TypeMetadata
	Indexes         map[string]*IndexMetadata
	Views           map[string]*ViewMetadata
}

// schema metadata for a table (a.k.a. column family)
type TableMetadata struct {
	Keyspace          string
	Name              string
	PartitionKey      []*ColumnMetadata
	ClusteringColumns []*ColumnMetadata
	Columns           map[string]*ColumnMetadata
	OrderedColumns    []string
	Options           TableMetadataOptions
	Flags             []string
	Extensions        map[string]interface{}
}

type TableMetadataOptions struct {
	BloomFilterFpChance     float64
	Caching                 map[string]string
	Comment                 string
	Compaction              map[string]string
	Compression             map[string]string
	CrcCheckChance          float64
	DcLocalReadRepairChance float64
	DefaultTimeToLive       int
	GcGraceSeconds          int
	MaxIndexInterval        int
	MemtableFlushPeriodInMs int
	MinIndexInterval        int
	ReadRepairChance        float64
	SpeculativeRetry        string
	CDC                     map[string]string
	InMemory                bool
	Version                 string
}

type ViewMetadata struct {
	KeyspaceName      string
	ViewName          string
	BaseTableID       string
	BaseTableName     string
	ID                string
	IncludeAllColumns bool
	Columns           map[string]*ColumnMetadata
	OrderedColumns    []string
	PartitionKey      []*ColumnMetadata
	ClusteringColumns []*ColumnMetadata
	WhereClause       string
	Options           TableMetadataOptions
	Extensions        map[string]interface{}
}

// schema metadata for a column
type ColumnMetadata struct {
	Keyspace        string
	Table           string
	Name            string
	ComponentIndex  int
	Kind            ColumnKind
	Type            string
	ClusteringOrder string
	Order           ColumnOrder
	Index           ColumnIndexMetadata
}

// FunctionMetadata holds metadata for function constructs
type FunctionMetadata struct {
	Keyspace          string
	Name              string
	ArgumentTypes     []string
	ArgumentNames     []string
	Body              string
	CalledOnNullInput bool
	Language          string
	ReturnType        string
}

// AggregateMetadata holds metadata for aggregate constructs
type AggregateMetadata struct {
	Keyspace      string
	Name          string
	ArgumentTypes []string
	FinalFunc     FunctionMetadata
	InitCond      string
	ReturnType    string
	StateFunc     FunctionMetadata
	StateType     string

	stateFunc string
	finalFunc string
}

// TypeMetadata holds the metadata for views.
type TypeMetadata struct {
	Keyspace   string
	Name       string
	FieldNames []string
	FieldTypes []string
}

type IndexMetadata struct {
	Name         string
	KeyspaceName string
	TableName    string
	Kind         string
	Options      map[string]string
}

const (
	IndexKindCustom = "CUSTOM"
)

const (
	TableFlagDense    = "dense"
	TableFlagSuper    = "super"
	TableFlagCompound = "compound"
)

// the ordering of the column with regard to its comparator
type ColumnOrder bool

const (
	ASC  ColumnOrder = false
	DESC             = true
)

type ColumnIndexMetadata struct {
	Name    string
	Type    string
	Options map[string]interface{}
}

type ColumnKind int

const (
	ColumnUnkownKind ColumnKind = iota
	ColumnPartitionKey
	ColumnClusteringKey
	ColumnRegular
	ColumnCompact
	ColumnStatic
)

func (c ColumnKind) String() string {
	switch c {
	case ColumnPartitionKey:
		return "partition_key"
	case ColumnClusteringKey:
		return "clustering_key"
	case ColumnRegular:
		return "regular"
	case ColumnCompact:
		return "compact"
	case ColumnStatic:
		return "static"
	default:
		return fmt.Sprintf("unknown_column_%d", c)
	}
}

func (c *ColumnKind) UnmarshalCQL(typ TypeInfo, p []byte) error {
	if typ.Type() != TypeVarchar {
		return unmarshalErrorf("unable to marshall %s into ColumnKind, expected Varchar", typ)
	}

	kind, err := columnKindFromSchema(string(p))
	if err != nil {
		return err
	}
	*c = kind

	return nil
}

func columnKindFromSchema(kind string) (ColumnKind, error) {
	switch kind {
	case "partition_key":
		return ColumnPartitionKey, nil
	case "clustering_key", "clustering":
		return ColumnClusteringKey, nil
	case "regular":
		return ColumnRegular, nil
	case "compact_value":
		return ColumnCompact, nil
	case "static":
		return ColumnStatic, nil
	default:
		return -1, fmt.Errorf("unknown column kind: %q", kind)
	}
}

// queries the cluster for schema information for a specific keyspace
type schemaDescriber struct {
	session *Session
	mu      sync.Mutex

	cache map[string]*KeyspaceMetadata
}

// creates a session bound schema describer which will query and cache
// keyspace metadata
func newSchemaDescriber(session *Session) *schemaDescriber {
	return &schemaDescriber{
		session: session,
		cache:   map[string]*KeyspaceMetadata{},
	}
}

// returns the cached KeyspaceMetadata held by the describer for the named
// keyspace.
func (s *schemaDescriber) getSchema(keyspaceName string) (*KeyspaceMetadata, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	metadata, found := s.cache[keyspaceName]
	if !found {
		// refresh the cache for this keyspace
		err := s.refreshSchema(keyspaceName)
		if err != nil {
			return nil, err
		}

		metadata = s.cache[keyspaceName]
	}

	return metadata, nil
}

// clears the already cached keyspace metadata
func (s *schemaDescriber) clearSchema(keyspaceName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.cache, keyspaceName)
}

// forcibly updates the current KeyspaceMetadata held by the schema describer
// for a given named keyspace.
func (s *schemaDescriber) refreshSchema(keyspaceName string) error {
	var err error

	// query the system keyspace for schema data
	// TODO retrieve concurrently
	keyspace, err := getKeyspaceMetadata(s.session, keyspaceName)
	if err != nil {
		return err
	}
	tables, err := getTableMetadata(s.session, keyspaceName)
	if err != nil {
		return err
	}
	columns, err := getColumnMetadata(s.session, keyspaceName)
	if err != nil {
		return err
	}
	functions, err := getFunctionsMetadata(s.session, keyspaceName)
	if err != nil {
		return err
	}
	aggregates, err := getAggregatesMetadata(s.session, keyspaceName)
	if err != nil {
		return err
	}
	types, err := getTypeMetadata(s.session, keyspaceName)
	if err != nil {
		return err
	}
	indexes, err := getIndexMetadata(s.session, keyspaceName)
	if err != nil {
		return err
	}
	views, err := getViewMetadata(s.session, keyspaceName)
	if err != nil {
		return err
	}

	// organize the schema data
	compileMetadata(keyspace, tables, columns, functions, aggregates, types, indexes, views)

	// update the cache
	s.cache[keyspaceName] = keyspace

	return nil
}

// "compiles" derived information about keyspace, table, and column metadata
// for a keyspace from the basic queried metadata objects returned by
// getKeyspaceMetadata, getTableMetadata, and getColumnMetadata respectively;
// Links the metadata objects together and derives the column composition of
// the partition key and clustering key for a table.
func compileMetadata(
	keyspace *KeyspaceMetadata,
	tables []TableMetadata,
	columns []ColumnMetadata,
	functions []FunctionMetadata,
	aggregates []AggregateMetadata,
	types []TypeMetadata,
	indexes []IndexMetadata,
	views []ViewMetadata,
) {
	keyspace.Tables = make(map[string]*TableMetadata)
	for i := range tables {
		tables[i].Columns = make(map[string]*ColumnMetadata)
		keyspace.Tables[tables[i].Name] = &tables[i]
	}
	keyspace.Functions = make(map[string]*FunctionMetadata, len(functions))
	for i := range functions {
		keyspace.Functions[functions[i].Name] = &functions[i]
	}
	keyspace.Aggregates = make(map[string]*AggregateMetadata, len(aggregates))
	for _, aggregate := range aggregates {
		aggregate.FinalFunc = *keyspace.Functions[aggregate.finalFunc]
		aggregate.StateFunc = *keyspace.Functions[aggregate.stateFunc]
		keyspace.Aggregates[aggregate.Name] = &aggregate
	}
	keyspace.Types = make(map[string]*TypeMetadata, len(types))
	for i := range types {
		keyspace.Types[types[i].Name] = &types[i]
	}
	keyspace.Indexes = make(map[string]*IndexMetadata, len(indexes))
	for i := range indexes {
		keyspace.Indexes[indexes[i].Name] = &indexes[i]
	}
	keyspace.Views = make(map[string]*ViewMetadata, len(views))
	for i := range views {
		v := &views[i]
		if _, ok := keyspace.Indexes[strings.TrimSuffix(v.ViewName, "_index")]; ok {
			continue
		}

		v.Columns = make(map[string]*ColumnMetadata)
		keyspace.Views[v.ViewName] = v
	}

	// add columns from the schema data
	for i := range columns {
		col := &columns[i]
		col.Order = ASC
		if col.ClusteringOrder == "desc" {
			col.Order = DESC
		}

		table, ok := keyspace.Tables[col.Table]
		if !ok {
			view, ok := keyspace.Views[col.Table]
			if !ok {
				// if the schema is being updated we will race between seeing
				// the metadata be complete. Potentially we should check for
				// schema versions before and after reading the metadata and
				// if they dont match try again.
				continue
			}

			view.Columns[col.Name] = col
			view.OrderedColumns = append(view.OrderedColumns, col.Name)
			continue
		}

		table.Columns[col.Name] = col
		table.OrderedColumns = append(table.OrderedColumns, col.Name)
	}

	for i := range tables {
		t := &tables[i]
		t.PartitionKey, t.ClusteringColumns, t.OrderedColumns = compileColumns(t.Columns, t.OrderedColumns)
	}
	for i := range views {
		v := &views[i]
		v.PartitionKey, v.ClusteringColumns, v.OrderedColumns = compileColumns(v.Columns, v.OrderedColumns)
	}
}

func compileColumns(columns map[string]*ColumnMetadata, orderedColumns []string) (
	partitionKey, clusteringColumns []*ColumnMetadata, sortedColumns []string) {
	clusteringColumnCount := componentColumnCountOfType(columns, ColumnClusteringKey)
	clusteringColumns = make([]*ColumnMetadata, clusteringColumnCount)

	partitionKeyCount := componentColumnCountOfType(columns, ColumnPartitionKey)
	partitionKey = make([]*ColumnMetadata, partitionKeyCount)

	var otherColumns []string
	for _, columnName := range orderedColumns {
		column := columns[columnName]
		if column.Kind == ColumnPartitionKey {
			partitionKey[column.ComponentIndex] = column
		} else if column.Kind == ColumnClusteringKey {
			clusteringColumns[column.ComponentIndex] = column
		} else {
			otherColumns = append(otherColumns, columnName)
		}
	}

	sortedColumns = orderedColumns[:0]
	for _, pk := range partitionKey {
		sortedColumns = append(sortedColumns, pk.Name)
	}
	for _, ck := range clusteringColumns {
		sortedColumns = append(sortedColumns, ck.Name)
	}
	for _, oc := range otherColumns {
		sortedColumns = append(sortedColumns, oc)
	}

	return
}

// returns the count of coluns with the given "kind" value.
func componentColumnCountOfType(columns map[string]*ColumnMetadata, kind ColumnKind) int {
	maxComponentIndex := -1
	for _, column := range columns {
		if column.Kind == kind && column.ComponentIndex > maxComponentIndex {
			maxComponentIndex = column.ComponentIndex
		}
	}
	return maxComponentIndex + 1
}

// query for keyspace metadata in the system_schema.keyspaces
func getKeyspaceMetadata(session *Session, keyspaceName string) (*KeyspaceMetadata, error) {
	if !session.useSystemSchema {
		return nil, ErrKeyspaceDoesNotExist
	}
	keyspace := &KeyspaceMetadata{Name: keyspaceName}

	const stmt = `
		SELECT durable_writes, replication
		FROM system_schema.keyspaces
		WHERE keyspace_name = ?`

	var replication map[string]string

	iter := session.control.query(stmt, keyspaceName)
	if iter.NumRows() == 0 {
		return nil, ErrKeyspaceDoesNotExist
	}
	iter.Scan(&keyspace.DurableWrites, &replication)
	err := iter.Close()
	if err != nil {
		return nil, fmt.Errorf("error querying keyspace schema: %v", err)
	}

	keyspace.StrategyClass = replication["class"]
	delete(replication, "class")

	keyspace.StrategyOptions = make(map[string]interface{}, len(replication))
	for k, v := range replication {
		keyspace.StrategyOptions[k] = v
	}

	return keyspace, nil
}

// query for table metadata in the system_schema.tables and system_schema.scylla_tables
func getTableMetadata(session *Session, keyspaceName string) ([]TableMetadata, error) {
	if !session.useSystemSchema {
		return nil, nil
	}

	stmt := `SELECT * FROM system_schema.tables WHERE keyspace_name = ?`
	iter := session.control.query(stmt, keyspaceName)

	var tables []TableMetadata
	table := TableMetadata{Keyspace: keyspaceName}
	for iter.MapScan(map[string]interface{}{
		"table_name":                  &table.Name,
		"bloom_filter_fp_chance":      &table.Options.BloomFilterFpChance,
		"caching":                     &table.Options.Caching,
		"comment":                     &table.Options.Comment,
		"compaction":                  &table.Options.Compaction,
		"compression":                 &table.Options.Compression,
		"crc_check_chance":            &table.Options.CrcCheckChance,
		"dclocal_read_repair_chance":  &table.Options.DcLocalReadRepairChance,
		"default_time_to_live":        &table.Options.DefaultTimeToLive,
		"gc_grace_seconds":            &table.Options.GcGraceSeconds,
		"max_index_interval":          &table.Options.MaxIndexInterval,
		"memtable_flush_period_in_ms": &table.Options.MemtableFlushPeriodInMs,
		"min_index_interval":          &table.Options.MinIndexInterval,
		"read_repair_chance":          &table.Options.ReadRepairChance,
		"speculative_retry":           &table.Options.SpeculativeRetry,
		"flags":                       &table.Flags,
		"extensions":                  &table.Extensions,
	}) {
		tables = append(tables, table)
		table = TableMetadata{Keyspace: keyspaceName}
	}

	err := iter.Close()
	if err != nil && err != ErrNotFound {
		return nil, fmt.Errorf("error querying table schema: %v", err)
	}

	stmt = `SELECT * FROM system_schema.scylla_tables WHERE keyspace_name = ? AND table_name = ?`
	for i, t := range tables {
		iter := session.control.query(stmt, keyspaceName, t.Name)

		table := TableMetadata{}
		if iter.MapScan(map[string]interface{}{
			"cdc":       &table.Options.CDC,
			"in_memory": &table.Options.InMemory,
			"version":   &table.Options.Version,
		}) {
			tables[i].Options.CDC = table.Options.CDC
			tables[i].Options.Version = table.Options.Version
			tables[i].Options.InMemory = table.Options.InMemory
		}
		if err := iter.Close(); err != nil && err != ErrNotFound {
			return nil, fmt.Errorf("error querying scylla table schema: %v", err)
		}
	}

	return tables, nil
}

// query for column metadata in the system_schema.columns
func getColumnMetadata(session *Session, keyspaceName string) ([]ColumnMetadata, error) {
	const stmt = `SELECT * FROM system_schema.columns WHERE keyspace_name = ?`

	var columns []ColumnMetadata

	iter := session.control.query(stmt, keyspaceName)
	column := ColumnMetadata{Keyspace: keyspaceName}

	for iter.MapScan(map[string]interface{}{
		"table_name":       &column.Table,
		"column_name":      &column.Name,
		"clustering_order": &column.ClusteringOrder,
		"type":             &column.Type,
		"kind":             &column.Kind,
		"position":         &column.ComponentIndex,
	}) {
		columns = append(columns, column)
		column = ColumnMetadata{Keyspace: keyspaceName}
	}

	if err := iter.Close(); err != nil && err != ErrNotFound {
		return nil, fmt.Errorf("error querying column schema: %v", err)
	}

	return columns, nil
}

func getTypeInfo(t string) TypeInfo {
	if strings.HasPrefix(t, apacheCassandraTypePrefix) {
		t = apacheToCassandraType(t)
	}
	return getCassandraType(t)
}

// query for type metadata in the system_schema.types
func getTypeMetadata(session *Session, keyspaceName string) ([]TypeMetadata, error) {
	if !session.useSystemSchema {
		return nil, nil
	}

	stmt := `SELECT * FROM system_schema.types WHERE keyspace_name = ?`
	iter := session.control.query(stmt, keyspaceName)

	var types []TypeMetadata
	tm := TypeMetadata{Keyspace: keyspaceName}

	for iter.MapScan(map[string]interface{}{
		"type_name":   &tm.Name,
		"field_names": &tm.FieldNames,
		"field_types": &tm.FieldTypes,
	}) {
		types = append(types, tm)
		tm = TypeMetadata{Keyspace: keyspaceName}
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return types, nil
}

// query for function metadata in the system_schema.functions
func getFunctionsMetadata(session *Session, keyspaceName string) ([]FunctionMetadata, error) {
	if !session.hasAggregatesAndFunctions || !session.useSystemSchema {
		return nil, nil
	}
	stmt := `SELECT * FROM system_schema.functions WHERE keyspace_name = ?`

	var functions []FunctionMetadata
	function := FunctionMetadata{Keyspace: keyspaceName}

	iter := session.control.query(stmt, keyspaceName)
	for iter.MapScan(map[string]interface{}{
		"function_name":        &function.Name,
		"argument_types":       &function.ArgumentTypes,
		"argument_names":       &function.ArgumentNames,
		"body":                 &function.Body,
		"called_on_null_input": &function.CalledOnNullInput,
		"language":             &function.Language,
		"return_type":          &function.ReturnType,
	}) {
		functions = append(functions, function)
		function = FunctionMetadata{Keyspace: keyspaceName}
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return functions, nil
}

// query for aggregate metadata in the system_schema.aggregates
func getAggregatesMetadata(session *Session, keyspaceName string) ([]AggregateMetadata, error) {
	if !session.hasAggregatesAndFunctions || !session.useSystemSchema {
		return nil, nil
	}

	const stmt = `SELECT * FROM system_schema.aggregates WHERE keyspace_name = ?`

	var aggregates []AggregateMetadata
	aggregate := AggregateMetadata{Keyspace: keyspaceName}

	iter := session.control.query(stmt, keyspaceName)
	for iter.MapScan(map[string]interface{}{
		"aggregate_name": &aggregate.Name,
		"argument_types": &aggregate.ArgumentTypes,
		"final_func":     &aggregate.finalFunc,
		"initcond":       &aggregate.InitCond,
		"return_type":    &aggregate.ReturnType,
		"state_func":     &aggregate.stateFunc,
		"state_type":     &aggregate.StateType,
	}) {
		aggregates = append(aggregates, aggregate)
		aggregate = AggregateMetadata{Keyspace: keyspaceName}
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return aggregates, nil
}

// query for index metadata in the system_schema.indexes
func getIndexMetadata(session *Session, keyspaceName string) ([]IndexMetadata, error) {
	if !session.useSystemSchema {
		return nil, nil
	}

	const stmt = `SELECT * FROM system_schema.indexes WHERE keyspace_name = ?`

	var indexes []IndexMetadata
	index := IndexMetadata{}

	iter := session.control.query(stmt, keyspaceName)
	for iter.MapScan(map[string]interface{}{
		"index_name":    &index.Name,
		"keyspace_name": &index.KeyspaceName,
		"table_name":    &index.TableName,
		"kind":          &index.Kind,
		"options":       &index.Options,
	}) {
		indexes = append(indexes, index)
		index = IndexMetadata{}
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return indexes, nil
}

// query for view metadata in the system_schema.views
func getViewMetadata(session *Session, keyspaceName string) ([]ViewMetadata, error) {
	if !session.useSystemSchema {
		return nil, nil
	}

	stmt := `SELECT * FROM system_schema.views WHERE keyspace_name = ?`

	iter := session.control.query(stmt, keyspaceName)

	var views []ViewMetadata
	view := ViewMetadata{KeyspaceName: keyspaceName}

	for iter.MapScan(map[string]interface{}{
		"id":                          &view.ID,
		"view_name":                   &view.ViewName,
		"base_table_id":               &view.BaseTableID,
		"base_table_name":             &view.BaseTableName,
		"include_all_columns":         &view.IncludeAllColumns,
		"where_clause":                &view.WhereClause,
		"bloom_filter_fp_chance":      &view.Options.BloomFilterFpChance,
		"caching":                     &view.Options.Caching,
		"comment":                     &view.Options.Comment,
		"compaction":                  &view.Options.Compaction,
		"compression":                 &view.Options.Compression,
		"crc_check_chance":            &view.Options.CrcCheckChance,
		"dclocal_read_repair_chance":  &view.Options.DcLocalReadRepairChance,
		"default_time_to_live":        &view.Options.DefaultTimeToLive,
		"gc_grace_seconds":            &view.Options.GcGraceSeconds,
		"max_index_interval":          &view.Options.MaxIndexInterval,
		"memtable_flush_period_in_ms": &view.Options.MemtableFlushPeriodInMs,
		"min_index_interval":          &view.Options.MinIndexInterval,
		"read_repair_chance":          &view.Options.ReadRepairChance,
		"speculative_retry":           &view.Options.SpeculativeRetry,
		"extensions":                  &view.Extensions,
	}) {
		views = append(views, view)
		view = ViewMetadata{KeyspaceName: keyspaceName}
	}

	err := iter.Close()
	if err != nil && err != ErrNotFound {
		return nil, fmt.Errorf("error querying view schema: %v", err)
	}

	return views, nil
}
