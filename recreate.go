// +build !cassandra
// Copyright (C) 2017 ScyllaDB

package gocql

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/template"
)

// ToCQL returns a CQL query that ca be used to recreate keyspace with all
// user defined types, tables, indexes, functions, aggregates and views associated
// with this keyspace.
func (km *KeyspaceMetadata) ToCQL() (string, error) {
	var sb strings.Builder

	if err := km.keyspaceToCQL(&sb); err != nil {
		return "", err
	}

	sortedTypes := km.typesSortedTopologically()
	for _, tm := range sortedTypes {
		if err := km.userTypeToCQL(&sb, tm); err != nil {
			return "", err
		}
	}

	for _, tm := range km.Tables {
		if err := km.tableToCQL(&sb, km.Name, tm); err != nil {
			return "", err
		}
	}

	for _, im := range km.Indexes {
		if err := km.indexToCQL(&sb, im); err != nil {
			return "", err
		}
	}

	for _, fm := range km.Functions {
		if err := km.functionToCQL(&sb, km.Name, fm); err != nil {
			return "", err
		}
	}

	for _, am := range km.Aggregates {
		if err := km.aggregateToCQL(&sb, am); err != nil {
			return "", err
		}
	}

	for _, vm := range km.Views {
		if err := km.viewToCQL(&sb, vm); err != nil {
			return "", err
		}
	}

	return sb.String(), nil
}

func (km *KeyspaceMetadata) typesSortedTopologically() []*TypeMetadata {
	sortedTypes := make([]*TypeMetadata, 0, len(km.Types))
	for _, tm := range km.Types {
		sortedTypes = append(sortedTypes, tm)
	}
	sort.Slice(sortedTypes, func(i, j int) bool {
		for _, ft := range sortedTypes[j].FieldTypes {
			if strings.Contains(ft, sortedTypes[i].Name) {
				return true
			}
		}
		return false
	})
	return sortedTypes
}

var tableCQLTemplate = template.Must(template.New("table").
	Funcs(map[string]interface{}{
		"escape":               cqlHelpers.escape,
		"tableColumnToCQL":     cqlHelpers.tableColumnToCQL,
		"tablePropertiesToCQL": cqlHelpers.tablePropertiesToCQL,
	}).
	Parse(`
CREATE TABLE {{ .KeyspaceName }}.{{ .Tm.Name }} (
  {{ tableColumnToCQL .Tm }}
) WITH {{ tablePropertiesToCQL .Tm.ClusteringColumns .Tm.Options .Tm.Flags .Tm.Extensions }};
`))

func (km *KeyspaceMetadata) tableToCQL(w io.Writer, kn string, tm *TableMetadata) error {
	if err := tableCQLTemplate.Execute(w, map[string]interface{}{
		"Tm":           tm,
		"KeyspaceName": kn,
	}); err != nil {
		return err
	}
	return nil
}

var functionTemplate = template.Must(template.New("functions").
	Funcs(map[string]interface{}{
		"escape":      cqlHelpers.escape,
		"zip":         cqlHelpers.zip,
		"stripFrozen": cqlHelpers.stripFrozen,
	}).
	Parse(`
CREATE FUNCTION {{ escape .keyspaceName }}.{{ escape .fm.Name }} ( 
  {{- range $i, $args := zip .fm.ArgumentNames .fm.ArgumentTypes }}
  {{- if ne $i 0 }}, {{ end }}
  {{- escape (index $args 0) }} {{ stripFrozen (index $args 1) }}
  {{- end -}})
  {{ if .fm.CalledOnNullInput }}CALLED{{ else }}RETURNS NULL{{ end }} ON NULL INPUT
  RETURNS {{ .fm.ReturnType }}
  LANGUAGE {{ .fm.Language }}
  AS $${{ .fm.Body }}$$;
`))

func (km *KeyspaceMetadata) functionToCQL(w io.Writer, keyspaceName string, fm *FunctionMetadata) error {
	if err := functionTemplate.Execute(w, map[string]interface{}{
		"fm":           fm,
		"keyspaceName": keyspaceName,
	}); err != nil {
		return err
	}
	return nil
}

var viewTemplate = template.Must(template.New("views").
	Funcs(map[string]interface{}{
		"zip":                  cqlHelpers.zip,
		"partitionKeyString":   cqlHelpers.partitionKeyString,
		"tablePropertiesToCQL": cqlHelpers.tablePropertiesToCQL,
	}).
	Parse(`
CREATE MATERIALIZED VIEW {{ .vm.KeyspaceName }}.{{ .vm.ViewName }} AS
SELECT {{ if .vm.IncludeAllColumns }}*{{ else }}
{{- range $i, $col := .vm.OrderedColumns }}
{{- if ne $i 0 }}, {{ end }}
{{- $col }}
{{- end }}
{{- end }}
FROM {{ .vm.KeyspaceName }}.{{ .vm.BaseTableName }}
WHERE {{ .vm.WhereClause }}
PRIMARY KEY ({{ partitionKeyString .vm.PartitionKey .vm.ClusteringColumns }})
WITH {{ tablePropertiesToCQL .vm.ClusteringColumns .vm.Options .flags .vm.Extensions }};
`))

func (km *KeyspaceMetadata) viewToCQL(w io.Writer, vm *ViewMetadata) error {
	if err := viewTemplate.Execute(w, map[string]interface{}{
		"vm":    vm,
		"flags": []string{},
	}); err != nil {
		return err
	}
	return nil
}

var aggregatesTemplate = template.Must(template.New("aggregate").
	Funcs(map[string]interface{}{
		"stripFrozen": cqlHelpers.stripFrozen,
	}).
	Parse(`
CREATE AGGREGATE {{ .Keyspace }}.{{ .Name }}( 
  {{- range $arg, $i := .ArgumentTypes }}
  {{- if ne $i 0 }}, {{ end }}
  {{- stripFrozen $arg }}
  {{- end -}})
  SFUNC {{ .StateFunc.Name }}
  STYPE {{ stripFrozen .State }}
  {{- if ne .FinalFunc.Name "" }}
  FINALFUNC {{ .FinalFunc.Name }}
  {{- end -}}
  {{- if ne .InitCond "" }}
  INITCOND {{ .InitCond }}
  {{- end -}}
);
`))

func (km *KeyspaceMetadata) aggregateToCQL(w io.Writer, am *AggregateMetadata) error {
	if err := aggregatesTemplate.Execute(w, am); err != nil {
		return err
	}
	return nil
}

var typeCQLTemplate = template.Must(template.New("types").
	Funcs(map[string]interface{}{
		"zip": cqlHelpers.zip,
	}).
	Parse(`
CREATE TYPE {{ .Keyspace }}.{{ .Name }} ( 
  {{- range $i, $fields := zip .FieldNames .FieldTypes }} {{- if ne $i 0 }},{{ end }}
  {{ index $fields 0 }} {{ index $fields 1 }}
  {{- end }}
);
`))

func (km *KeyspaceMetadata) userTypeToCQL(w io.Writer, tm *TypeMetadata) error {
	if err := typeCQLTemplate.Execute(w, tm); err != nil {
		return err
	}
	return nil
}

func (km *KeyspaceMetadata) indexToCQL(w io.Writer, im *IndexMetadata) error {
	// Scylla doesn't support any custom indexes
	if im.Kind == IndexKindCustom {
		return nil
	}

	options := im.Options
	indexTarget := options["target"]

	// secondary index
	si := struct {
		ClusteringKeys []string `json:"ck"`
		PartitionKeys  []string `json:"pk"`
	}{}

	if err := json.Unmarshal([]byte(indexTarget), &si); err == nil {
		indexTarget = fmt.Sprintf("(%s), %s",
			strings.Join(si.PartitionKeys, ","),
			strings.Join(si.ClusteringKeys, ","),
		)
	}

	_, err := fmt.Fprintf(w, "\nCREATE INDEX %s ON %s.%s (%s);\n",
		im.Name,
		im.KeyspaceName,
		im.TableName,
		indexTarget,
	)
	if err != nil {
		return err
	}

	return nil
}

var keyspaceCQLTemplate = template.Must(template.New("keyspace").
	Funcs(map[string]interface{}{
		"escape":      cqlHelpers.escape,
		"fixStrategy": cqlHelpers.fixStrategy,
	}).
	Parse(`
CREATE KEYSPACE {{ .Name }} WITH replication = {
  'class': {{ escape ( fixStrategy .StrategyClass) }}
  {{- range $key, $value := .StrategyOptions }},
  {{ escape $key }}: {{ escape $value }}
  {{- end }}
}{{ if not .DurableWrites }} AND durable_writes = 'false'{{ end }};
`))

func (km *KeyspaceMetadata) keyspaceToCQL(w io.Writer) error {
	if err := keyspaceCQLTemplate.Execute(w, km); err != nil {
		return err
	}
	return nil
}

func contains(in []string, v string) bool {
	for _, e := range in {
		if e == v {
			return true
		}
	}
	return false
}

type toCQLHelpers struct{}

var cqlHelpers = toCQLHelpers{}

func (h toCQLHelpers) zip(a []string, b []string) [][]string {
	m := make([][]string, len(a))
	for i := range a {
		m[i] = []string{a[i], b[i]}
	}
	return m
}

func (h toCQLHelpers) escape(e interface{}) string {
	switch v := e.(type) {
	case int, float64:
		return fmt.Sprint(v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case string:
		return "'" + strings.ReplaceAll(v, "'", "''") + "'"
	case []byte:
		return string(v)
	}
	return ""
}

func (h toCQLHelpers) stripFrozen(v string) string {
	return strings.TrimSuffix(strings.TrimPrefix(v, "frozen<"), ">")
}
func (h toCQLHelpers) fixStrategy(v string) string {
	return strings.TrimPrefix(v, "org.apache.cassandra.locator.")
}

func (h toCQLHelpers) fixQuote(v string) string {
	return strings.ReplaceAll(v, `"`, `'`)
}

func (h toCQLHelpers) tableOptionsToCQL(ops TableMetadataOptions) ([]string, error) {
	opts := map[string]interface{}{
		"bloom_filter_fp_chance":      ops.BloomFilterFpChance,
		"comment":                     ops.Comment,
		"crc_check_chance":            ops.CrcCheckChance,
		"dclocal_read_repair_chance":  ops.DcLocalReadRepairChance,
		"default_time_to_live":        ops.DefaultTimeToLive,
		"gc_grace_seconds":            ops.GcGraceSeconds,
		"max_index_interval":          ops.MaxIndexInterval,
		"memtable_flush_period_in_ms": ops.MemtableFlushPeriodInMs,
		"min_index_interval":          ops.MinIndexInterval,
		"read_repair_chance":          ops.ReadRepairChance,
		"speculative_retry":           ops.SpeculativeRetry,
	}

	var err error
	opts["caching"], err = json.Marshal(ops.Caching)
	if err != nil {
		return nil, err
	}

	opts["compaction"], err = json.Marshal(ops.Compaction)
	if err != nil {
		return nil, err
	}

	opts["compression"], err = json.Marshal(ops.Compression)
	if err != nil {
		return nil, err
	}

	cdc, err := json.Marshal(ops.CDC)
	if err != nil {
		return nil, err
	}

	if string(cdc) != "null" {
		opts["cdc"] = cdc
	}

	if ops.InMemory {
		opts["in_memory"] = ops.InMemory
	}

	out := make([]string, 0, len(opts))
	for key, opt := range opts {
		out = append(out, fmt.Sprintf("%s = %s", key, h.fixQuote(h.escape(opt))))
	}

	sort.Strings(out)
	return out, nil
}

func (h toCQLHelpers) tableExtensionsToCQL(extensions map[string]interface{}) ([]string, error) {
	exts := map[string]interface{}{}

	if blob, ok := extensions["scylla_encryption_options"]; ok {
		encOpts := &scyllaEncryptionOptions{}
		if err := encOpts.UnmarshalBinary(blob.([]byte)); err != nil {
			return nil, err
		}

		var err error
		exts["scylla_encryption_options"], err = json.Marshal(encOpts)
		if err != nil {
			return nil, err
		}

	}

	out := make([]string, 0, len(exts))
	for key, ext := range exts {
		out = append(out, fmt.Sprintf("%s = %s", key, h.fixQuote(h.escape(ext))))
	}

	sort.Strings(out)
	return out, nil
}

func (h toCQLHelpers) tablePropertiesToCQL(cks []*ColumnMetadata, opts TableMetadataOptions,
	flags []string, extensions map[string]interface{}) (string, error) {
	var sb strings.Builder

	var properties []string

	compactStorage := len(flags) > 0 && (contains(flags, TableFlagDense) ||
		contains(flags, TableFlagSuper) ||
		!contains(flags, TableFlagCompound))

	if compactStorage {
		properties = append(properties, "COMPACT STORAGE")
	}

	if len(cks) > 0 {
		var inner []string
		for _, col := range cks {
			inner = append(inner, fmt.Sprintf("%s %s", col.Name, col.ClusteringOrder))
		}
		properties = append(properties, fmt.Sprintf("CLUSTERING ORDER BY (%s)", strings.Join(inner, ", ")))
	}

	options, err := h.tableOptionsToCQL(opts)
	if err != nil {
		return "", err
	}
	properties = append(properties, options...)

	exts, err := h.tableExtensionsToCQL(extensions)
	if err != nil {
		return "", err
	}
	properties = append(properties, exts...)

	sb.WriteString(strings.Join(properties, "\n    AND "))
	return sb.String(), nil
}

func (h toCQLHelpers) tableColumnToCQL(tm *TableMetadata) string {
	var sb strings.Builder

	var columns []string
	for _, cn := range tm.OrderedColumns {
		cm := tm.Columns[cn]
		column := fmt.Sprintf("%s %s", cn, cm.Type)
		if cm.Kind == ColumnStatic {
			column += " static"
		}
		columns = append(columns, column)
	}
	if len(tm.PartitionKey) == 1 && len(tm.ClusteringColumns) == 0 && len(columns) > 0 {
		columns[0] += " PRIMARY KEY"
	}

	sb.WriteString(strings.Join(columns, ",\n  "))

	if len(tm.PartitionKey) > 1 || len(tm.ClusteringColumns) > 0 {
		sb.WriteString(",\n  PRIMARY KEY (")
		sb.WriteString(h.partitionKeyString(tm.PartitionKey, tm.ClusteringColumns))
		sb.WriteRune(')')
	}

	return sb.String()
}

func (h toCQLHelpers) partitionKeyString(pks, cks []*ColumnMetadata) string {
	var sb strings.Builder

	if len(pks) > 1 {
		sb.WriteRune('(')
		for i, pk := range pks {
			if i != 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(pk.Name)
		}
		sb.WriteRune(')')
	} else {
		sb.WriteString(pks[0].Name)
	}

	if len(cks) > 0 {
		sb.WriteString(", ")
		for i, ck := range cks {
			if i != 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(ck.Name)
		}
	}

	return sb.String()
}

type scyllaEncryptionOptions struct {
	CipherAlgorithm   string `json:"cipher_algorithm"`
	SecretKeyStrength int    `json:"secret_key_strength"`
	KeyProvider       string `json:"key_provider"`
	SecretKeyFile     string `json:"secret_key_file"`
}

// UnmarshalBinary deserializes blob into scyllaEncryptionOptions.
// Format:
//  * 4 bytes - size of KV map
//  Size times:
//  * 4 bytes - length of key
//  * len_of_key bytes - key
//  * 4 bytes - length of value
//  * len_of_value bytes - value
func (enc *scyllaEncryptionOptions) UnmarshalBinary(data []byte) error {
	size := binary.LittleEndian.Uint32(data[0:4])

	m := make(map[string]string, size)

	off := uint32(4)
	for i := uint32(0); i < size; i++ {
		keyLen := binary.LittleEndian.Uint32(data[off : off+4])
		off += 4

		key := string(data[off : off+keyLen])
		off += keyLen

		valueLen := binary.LittleEndian.Uint32(data[off : off+4])
		off += 4

		value := string(data[off : off+valueLen])
		off += valueLen

		m[key] = value
	}

	enc.CipherAlgorithm = m["cipher_algorithm"]
	enc.KeyProvider = m["key_provider"]
	enc.SecretKeyFile = m["secret_key_file"]
	if secretKeyStrength, ok := m["secret_key_strength"]; ok {
		sks, err := strconv.Atoi(secretKeyStrength)
		if err != nil {
			return err
		}
		enc.SecretKeyStrength = sks
	}

	return nil
}
