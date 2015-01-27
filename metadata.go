// Copyright (c) 2015 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
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
}

// schema metadata for a table (a.k.a. column family)
type TableMetadata struct {
	Keyspace          string
	Name              string
	KeyValidator      string
	Comparator        string
	DefaultValidator  string
	KeyAliases        []string
	ColumnAliases     []string
	ValueAlias        string
	PartitionKey      []*ColumnMetadata
	ClusteringColumns []*ColumnMetadata
	Columns           map[string]*ColumnMetadata
}

// schema metadata for a column
type ColumnMetadata struct {
	Keyspace       string
	Table          string
	Name           string
	Kind           string
	ComponentIndex int
	Type           TypeInfo
	Order          ColumnOrder
	Index          ColumnIndexMetadata
}

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

// Column kind values
const (
	PARTITION_KEY  = "partition_key"
	CLUSTERING_KEY = "clustering_key"
	REGULAR        = "regular"
	COMPACT_VALUE  = "compact_value"
	STATIC         = "static"
)

// column index type values
const (
	KEYS   = "KEYS"
	CUSTOM = "CUSTOM"
)

// default alias values
const (
	DEFAULT_KEY_ALIAS    = "key"
	DEFAULT_COLUMN_ALIAS = "column"
	DEFAULT_VALUE_ALIAS  = "value"
)

// queries the cluster for schema information for a specific keyspace
type schemaDescriber struct {
	session  *Session
	mu       sync.Mutex
	lazyInit sync.Once

	current *KeyspaceMetadata
	err     error
}

func (s *schemaDescriber) init() {
	s.err = s.RefreshSchema()
}

func (s *schemaDescriber) GetSchema() (*KeyspaceMetadata, error) {
	// lazily-initialize the schema the first time
	s.lazyInit.Do(s.init)

	// TODO handle schema change events

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.current, s.err
}

func (s *schemaDescriber) RefreshSchema() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var err error

	keyspace, err := s.getKeyspaceMetadata()

	tables, err := s.getTableMetadata()
	if err != nil {
		return err
	}
	columns, err := s.getColumnMetadata()
	if err != nil {
		return err
	}
	s.compileMetadata(keyspace, tables, columns)

	// update the current
	s.current = keyspace
	s.err = nil

	return nil
}

func (s *schemaDescriber) compileMetadata(
	keyspace *KeyspaceMetadata,
	tables []*TableMetadata,
	columns []*ColumnMetadata,
) {
	keyspace.Tables = make(map[string]*TableMetadata)
	for _, table := range tables {
		keyspace.Tables[table.Name] = table
	}

	// add columns from the schema data
	for _, column := range columns {
		table := keyspace.Tables[column.Table]
		table.Columns[column.Name] = column
	}

	for _, table := range tables {
		// decode the key validator
		keyValidatorParsed := parseType(table.KeyValidator)
		// decode the comparator
		comparatorParsed := parseType(table.Comparator)
		// decode the default validator
		defaultValidatorParsed := parseType(table.DefaultValidator)

		// the partition key length is the same as the number of types in the
		// key validator
		table.PartitionKey = make([]*ColumnMetadata, len(keyValidatorParsed.types))

		if s.session.cfg.ProtoVersion == 1 {
			// V1 protocol only returns "regular" columns from
			// system.schema_columns so the alias information is used to
			// create the partition key and clustering columns

			// construct the partition key from the alias
			for i := range table.PartitionKey {
				var alias string
				if len(table.KeyAliases) > i {
					alias = table.KeyAliases[i]
				} else if i == 0 {
					alias = DEFAULT_KEY_ALIAS
				} else {
					alias = DEFAULT_KEY_ALIAS + strconv.Itoa(i+1)
				}

				column := &ColumnMetadata{
					Keyspace: s.session.cfg.Keyspace,
					Table:    table.Name,
					Name:     alias,
					Type:     keyValidatorParsed.types[i],
				}

				table.PartitionKey[i] = column
				table.Columns[alias] = column
			}

			// determine the number of clustering columns
			size := len(comparatorParsed.types)
			if comparatorParsed.isComposite &&
				(len(comparatorParsed.collections) > 0 ||
					(len(table.ColumnAliases) == size-1 &&
						comparatorParsed.types[size-1].Type == TypeVarchar)) {

				size = size - 1
			} else if len(table.ColumnAliases) == 0 && len(table.Columns) > 0 {
				// only regular columns
				size = 0
			}
			table.ClusteringColumns = make([]*ColumnMetadata, size)

			for i := range table.ClusteringColumns {
				var alias string
				if len(table.ColumnAliases) > i {
					alias = table.ColumnAliases[i]
				} else if i == 0 {
					alias = DEFAULT_COLUMN_ALIAS
				} else {
					alias = DEFAULT_COLUMN_ALIAS + strconv.Itoa(i+1)
				}

				order := ASC
				if comparatorParsed.reversed[i] {
					order = DESC
				}

				column := &ColumnMetadata{
					Keyspace: s.session.cfg.Keyspace,
					Table:    table.Name,
					Name:     alias,
					Type:     comparatorParsed.types[i],
					Order:    order,
				}

				table.ClusteringColumns[i] = column
				table.Columns[alias] = column
			}

			if size != len(comparatorParsed.types)-1 {
				alias := DEFAULT_VALUE_ALIAS
				if len(table.ValueAlias) > 0 {
					alias = table.ValueAlias
				}
				column := &ColumnMetadata{
					Keyspace: s.session.cfg.Keyspace,
					Table:    table.Name,
					Name:     alias,
					Type:     defaultValidatorParsed.types[0],
				}
				table.Columns[alias] = column
			}
		} else {
			// V2+

			// find the largest index in the clustering key columns
			maxIndex := -1
			for _, column := range table.Columns {
				if column.Kind == CLUSTERING_KEY &&
					column.ComponentIndex > maxIndex {

					maxIndex = column.ComponentIndex
				}
			}
			table.ClusteringColumns = make([]*ColumnMetadata, maxIndex+1)

			for _, column := range table.Columns {
				if column.Kind == PARTITION_KEY {
					table.PartitionKey[column.ComponentIndex] = column
				} else if column.Kind == CLUSTERING_KEY {
					table.ClusteringColumns[column.ComponentIndex] = column
				}
			}
		}

	}
}

func (s *schemaDescriber) getKeyspaceMetadata() (*KeyspaceMetadata, error) {
	query := s.session.Query(
		`
		SELECT durable_writes, strategy_class, strategy_options
		FROM system.schema_keyspaces
		WHERE keyspace_name = ?
		`,
		s.session.cfg.Keyspace,
	)
	// set a routing key to avoid a token aware policy from creating a call loop.
	// TODO use a separate connection (pool) for system keyspace queries.
	query.RoutingKey([]byte{})

	keyspace := &KeyspaceMetadata{Name: s.session.cfg.Keyspace}
	var strategyOptionsJSON []byte

	err := query.Scan(
		&keyspace.DurableWrites,
		&keyspace.StrategyClass,
		&strategyOptionsJSON,
	)
	if err != nil {
		return nil, fmt.Errorf("Error querying keyspace schema: %v", err)
	}

	err = json.Unmarshal(strategyOptionsJSON, &keyspace.StrategyOptions)
	if err != nil {
		return nil, err
	}

	return keyspace, nil
}

func (s *schemaDescriber) getTableMetadata() ([]*TableMetadata, error) {
	query := s.session.Query(
		`
		SELECT
			columnfamily_name,
			key_validator,
			comparator,
			default_validator,
			key_aliases,
			column_aliases,
			value_alias
		FROM system.schema_columnfamilies
		WHERE keyspace_name = ?
		`,
		s.session.cfg.Keyspace,
	)
	// set a routing key to avoid a token aware policy from creating a call loop.
	// TODO use a separate connection (pool) for system keyspace queries.
	query.RoutingKey([]byte{})
	iter := query.Iter()

	tables := []*TableMetadata{}

	table := &TableMetadata{Keyspace: s.session.cfg.Keyspace}
	var keyAliasesJSON []byte
	var columnAliasesJSON []byte
	for iter.Scan(
		&table.Name,
		&table.KeyValidator,
		&table.Comparator,
		&table.DefaultValidator,
		&keyAliasesJSON,
		&columnAliasesJSON,
		&table.ValueAlias,
	) {
		var err error

		table.Columns = make(map[string]*ColumnMetadata)

		// decode the key aliases
		if keyAliasesJSON != nil {
			table.KeyAliases = []string{}
			err = json.Unmarshal(keyAliasesJSON, &table.KeyAliases)
			if err != nil {
				iter.Close()
				return nil, err
			}
		}

		// decode the column aliases
		if columnAliasesJSON != nil {
			table.ColumnAliases = []string{}
			err = json.Unmarshal(columnAliasesJSON, &table.ColumnAliases)
			if err != nil {
				iter.Close()
				return nil, err
			}
		}

		tables = append(tables, table)

		table = &TableMetadata{Keyspace: s.session.cfg.Keyspace}
	}

	err := iter.Close()
	if err != nil && err != ErrNotFound {
		return nil, fmt.Errorf("Error querying table schema: %v", err)
	}

	return tables, nil
}

func (s *schemaDescriber) getColumnMetadata() ([]*ColumnMetadata, error) {
	stmt := `
		SELECT
			columnfamily_name,
			column_name,
			component_index,
			validator,
			index_name,
			index_type,
			index_options,
			type
		FROM system.schema_columns
		WHERE keyspace_name = ?
		`
	if s.session.cfg.ProtoVersion == 1 {
		// V1 does not support the type column
		stmt = `
		SELECT
			columnfamily_name,
			column_name,
			component_index,
			validator,
			index_name,
			index_type,
			index_options
		FROM system.schema_columns
		WHERE keyspace_name = ?
		`
	}
	query := s.session.Query(
		stmt,
		s.session.cfg.Keyspace,
	)
	// set a routing key to avoid a token aware policy from creating a call loop.
	// TODO use a separate connection (pool) for system keyspace queries.
	query.RoutingKey([]byte{})
	iter := query.Iter()

	columns := []*ColumnMetadata{}

	column := &ColumnMetadata{Keyspace: s.session.cfg.Keyspace}
	var validator string
	var indexOptionsJSON []byte
	scan := func() bool {
		return iter.Scan(
			&column.Table,
			&column.Name,
			&column.ComponentIndex,
			&validator,
			&column.Index.Name,
			&column.Index.Type,
			&indexOptionsJSON,
			&column.Kind,
		)
	}
	if s.session.cfg.ProtoVersion == 1 {
		// V1 does not support the type column
		scan = func() bool {
			return iter.Scan(
				&column.Table,
				&column.Name,
				&column.ComponentIndex,
				&validator,
				&column.Index.Name,
				&column.Index.Type,
				&indexOptionsJSON,
			)
		}
	}
	for scan() {
		var err error

		// decode the validator
		validatorParsed := parseType(validator)

		// decode the index options
		if indexOptionsJSON != nil {
			err = json.Unmarshal(indexOptionsJSON, &column.Index.Options)
			if err != nil {
				iter.Close()
				return nil, err
			}
		}

		column.Type = validatorParsed.types[0]
		column.Order = ASC
		if validatorParsed.reversed[0] {
			column.Order = DESC
		}

		columns = append(columns, column)

		column = &ColumnMetadata{Keyspace: s.session.cfg.Keyspace}
	}

	err := iter.Close()
	if err != nil && err != ErrNotFound {
		return nil, fmt.Errorf("Error querying column schema: %v", err)
	}

	return columns, nil
}

// type definition parser state
type typeParser struct {
	input string
	index int
}

// the type definition parser result
type typeParserResult struct {
	isComposite bool
	types       []TypeInfo
	reversed    []bool
	collections map[string]TypeInfo
}

// Parse the type definition used for validator and comparator schema data
func parseType(def string) typeParserResult {
	parser := &typeParser{input: def}
	return parser.parse()
}

const (
	REVERSED_TYPE   = "org.apache.cassandra.db.marshal.ReversedType"
	COMPOSITE_TYPE  = "org.apache.cassandra.db.marshal.CompositeType"
	COLLECTION_TYPE = "org.apache.cassandra.db.marshal.ColumnToCollectionType"
	LIST_TYPE       = "org.apache.cassandra.db.marshal.ListType"
	SET_TYPE        = "org.apache.cassandra.db.marshal.SetType"
	MAP_TYPE        = "org.apache.cassandra.db.marshal.MapType"
	UDT_TYPE        = "org.apache.cassandra.db.marshal.UserType"
	TUPLE_TYPE      = "org.apache.cassandra.db.marshal.TupleType"
)

// represents a class specification in the type def AST
type typeParserClassNode struct {
	name   string
	params []typeParserParamNode
	// this is the segment of the input string that defined this node
	input string
}

// represents a class parameter in the type def AST
type typeParserParamNode struct {
	name  *string
	class typeParserClassNode
}

func (t *typeParser) parse() typeParserResult {
	// parse the AST
	ast, ok := t.parseClassNode()
	if !ok {
		// treat this is a custom type
		return typeParserResult{
			isComposite: false,
			types: []TypeInfo{
				TypeInfo{
					Type:   TypeCustom,
					Custom: t.input,
				},
			},
			reversed:    []bool{false},
			collections: nil,
		}
	}

	// interpret the AST
	if strings.HasPrefix(ast.name, COMPOSITE_TYPE) {
		count := len(ast.params)

		// look for a collections param
		last := ast.params[count-1]
		collections := map[string]TypeInfo{}
		if strings.HasPrefix(last.class.name, COLLECTION_TYPE) {
			count--

			for _, param := range last.class.params {
				// decode the name
				var name string
				decoded, err := hex.DecodeString(*param.name)
				if err != nil {
					log.Printf(
						"Error parsing type '%s', contains collection name '%s' with an invalid format: %v",
						t.input,
						*param.name,
						err,
					)
					// just use the provided name
					name = *param.name
				} else {
					name = string(decoded)
				}
				collections[name] = param.class.asTypeInfo()
			}
		}

		types := make([]TypeInfo, count)
		reversed := make([]bool, count)

		for i, param := range ast.params[:count] {
			class := param.class
			reversed[i] = strings.HasPrefix(class.name, REVERSED_TYPE)
			if reversed[i] {
				class = class.params[0].class
			}
			types[i] = class.asTypeInfo()
		}

		return typeParserResult{
			isComposite: true,
			types:       types,
			reversed:    reversed,
			collections: collections,
		}
	} else {
		// not composite, so one type
		class := *ast
		reversed := strings.HasPrefix(class.name, REVERSED_TYPE)
		if reversed {
			class = class.params[0].class
		}
		typeInfo := class.asTypeInfo()

		return typeParserResult{
			isComposite: false,
			types:       []TypeInfo{typeInfo},
			reversed:    []bool{reversed},
		}
	}
}

func (class *typeParserClassNode) asTypeInfo() TypeInfo {
	if strings.HasPrefix(class.name, LIST_TYPE) {
		elem := class.params[0].class.asTypeInfo()
		return TypeInfo{
			Type: TypeList,
			Elem: &elem,
		}
	}
	if strings.HasPrefix(class.name, SET_TYPE) {
		elem := class.params[0].class.asTypeInfo()
		return TypeInfo{
			Type: TypeSet,
			Elem: &elem,
		}
	}
	if strings.HasPrefix(class.name, MAP_TYPE) {
		key := class.params[0].class.asTypeInfo()
		elem := class.params[1].class.asTypeInfo()
		return TypeInfo{
			Type: TypeMap,
			Key:  &key,
			Elem: &elem,
		}
	}
	// TODO handle UDT types
	// TODO handle Tuple types

	// must be a simple type or custom type
	info := TypeInfo{Type: getApacheCassandraType(class.name)}
	if info.Type == TypeCustom {
		// add the entire class definition
		info.Custom = class.input
	}
	return info
}

// CLASS := ID [ PARAMS ]
func (t *typeParser) parseClassNode() (node *typeParserClassNode, ok bool) {
	t.skipWhitespace()

	startIndex := t.index

	name, ok := t.nextIdentifier()
	if !ok {
		return nil, false
	}

	params, ok := t.parseParamNodes()
	if !ok {
		return nil, false
	}

	endIndex := t.index

	node = &typeParserClassNode{
		name:   name,
		params: params,
		input:  t.input[startIndex:endIndex],
	}
	return node, true
}

// PARAMS := "(" PARAM { "," PARAM } ")"
// PARAM := [ PARAM_NAME ":" ] CLASS
// PARAM_NAME := ID
func (t *typeParser) parseParamNodes() (params []typeParserParamNode, ok bool) {
	t.skipWhitespace()

	// the params are optional
	if t.index == len(t.input) || t.input[t.index] != '(' {
		return nil, true
	}

	params = []typeParserParamNode{}

	// consume the '('
	t.index++

	t.skipWhitespace()

	for t.input[t.index] != ')' {
		// look for a named param, but if no colon, then we want to backup
		backupIndex := t.index

		// name will be a hex encoded version of a utf-8 string
		name, ok := t.nextIdentifier()
		if !ok {
			return nil, false
		}
		hasName := true

		// TODO handle '=>' used for DynamicCompositeType

		t.skipWhitespace()

		if t.input[t.index] == ':' {
			// there is a name for this parameter

			// consume the ':'
			t.index++

			t.skipWhitespace()
		} else {
			// no name, backup
			hasName = false
			t.index = backupIndex
		}

		// parse the next full parameter
		classNode, ok := t.parseClassNode()
		if !ok {
			return nil, false
		}

		if hasName {
			params = append(
				params,
				typeParserParamNode{name: &name, class: *classNode},
			)
		} else {
			params = append(
				params,
				typeParserParamNode{class: *classNode},
			)
		}

		t.skipWhitespace()

		if t.input[t.index] == ',' {
			// consume the comma
			t.index++

			t.skipWhitespace()
		}
	}

	// consume the ')'
	t.index++

	return params, true
}

func (t *typeParser) skipWhitespace() {
	for t.index < len(t.input) && isWhitespaceChar(t.input[t.index]) {
		t.index++
	}
}

func isWhitespaceChar(c byte) bool {
	return c == ' ' || c == '\n' || c == '\t'
}

// ID := LETTER { LETTER }
// LETTER := "0"..."9" | "a"..."z" | "A"..."Z" | "-" | "+" | "." | "_" | "&"
func (t *typeParser) nextIdentifier() (id string, found bool) {
	startIndex := t.index
	for t.index < len(t.input) && isIdentifierChar(t.input[t.index]) {
		t.index++
	}
	if startIndex == t.index {
		return "", false
	}
	return t.input[startIndex:t.index], true
}

func isIdentifierChar(c byte) bool {
	return (c >= '0' && c <= '9') ||
		(c >= 'a' && c <= 'z') ||
		(c >= 'A' && c <= 'Z') ||
		c == '-' ||
		c == '+' ||
		c == '.' ||
		c == '_' ||
		c == '&'
}
