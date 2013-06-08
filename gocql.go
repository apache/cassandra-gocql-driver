// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The gocql package provides a database/sql driver for CQL, the Cassandra
// query language.
//
// This package requires a recent version of Cassandra (≥ 1.2) that supports
// CQL 3.0 and the new native protocol. The native protocol is still considered
// beta and must be enabled manually in Cassandra 1.2 by setting
// "start_native_transport" to true in conf/cassandra.yaml.
//
// Example Usage:
//
//     db, err := sql.Open("gocql", "localhost:9042 keyspace=system")
//     // ...
//     rows, err := db.Query("SELECT keyspace_name FROM schema_keyspaces")
//     // ...
//     for rows.Next() {
//          var keyspace string
//          err = rows.Scan(&keyspace)
//          // ...
//          fmt.Println(keyspace)
//     }
//     if err := rows.Err(); err != nil {
//         // ...
//     }
//
package gocql

import (
	"bytes"
	"code.google.com/p/snappy-go/snappy"
	"database/sql"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strings"
)

const (
	protoRequest  byte = 0x01
	protoResponse byte = 0x81

	opError        byte = 0x00
	opStartup      byte = 0x01
	opReady        byte = 0x02
	opAuthenticate byte = 0x03
	opCredentials  byte = 0x04
	opOptions      byte = 0x05
	opSupported    byte = 0x06
	opQuery        byte = 0x07
	opResult       byte = 0x08
	opPrepare      byte = 0x09
	opExecute      byte = 0x0A
	opLAST         byte = 0x0A // not a real opcode -- used to check for valid opcodes

	flagCompressed byte = 0x01

	keyVersion     string = "CQL_VERSION"
	keyCompression string = "COMPRESSION"
)

var consistencyLevels = map[string]byte{"any": 0x00, "one": 0x01, "two": 0x02,
	"three": 0x03, "quorum": 0x04, "all": 0x05, "local_quorum": 0x06, "each_quorum": 0x07}

var rnd = rand.New(rand.NewSource(0))

type drv struct{}

func (d drv) Open(name string) (driver.Conn, error) {
	return Open(name)
}

type connection struct {
	c           net.Conn
	compression string
	consistency byte
}

func Open(name string) (*connection, error) {
	parts := strings.Split(name, " ")
	address := ""
	if len(parts) >= 1 {
		addresses := strings.Split(parts[0], ",")
		if len(addresses) > 0 {
			address = addresses[rnd.Intn(len(addresses))]
		}
	}
	c, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	version := "3.0.0"
	var (
		keyspace    string
		compression string
		consistency byte = 0x01
		ok          bool
	)
	for i := 1; i < len(parts); i++ {
		switch {
		case parts[i] == "":
			continue
		case strings.HasPrefix(parts[i], "keyspace="):
			keyspace = strings.TrimSpace(parts[i][9:])
		case strings.HasPrefix(parts[i], "compression="):
			compression = strings.TrimSpace(parts[i][12:])
			if compression != "snappy" {
				return nil, fmt.Errorf("unknown compression algorithm %q",
					compression)
			}
		case strings.HasPrefix(parts[i], "version="):
			version = strings.TrimSpace(parts[i][8:])
		case strings.HasPrefix(parts[i], "consistency="):
			cs := strings.TrimSpace(parts[i][12:])
			if consistency, ok = consistencyLevels[cs]; !ok {
				return nil, fmt.Errorf("unknown consistency level %q", cs)
			}
		default:
			return nil, fmt.Errorf("unsupported option %q", parts[i])
		}
	}

	cn := &connection{c: c, compression: compression, consistency: consistency}

	b := &bytes.Buffer{}

	if compression != "" {
		binary.Write(b, binary.BigEndian, uint16(2))
	} else {
		binary.Write(b, binary.BigEndian, uint16(1))
	}

	binary.Write(b, binary.BigEndian, uint16(len(keyVersion)))
	b.WriteString(keyVersion)
	binary.Write(b, binary.BigEndian, uint16(len(version)))
	b.WriteString(version)

	if compression != "" {
		binary.Write(b, binary.BigEndian, uint16(len(keyCompression)))
		b.WriteString(keyCompression)
		binary.Write(b, binary.BigEndian, uint16(len(compression)))
		b.WriteString(compression)
	}

	if err := cn.send(opStartup, b.Bytes()); err != nil {
		return nil, err
	}

	opcode, _, err := cn.recv()
	if err != nil {
		return nil, err
	}
	if opcode != opReady {
		return nil, fmt.Errorf("connection not ready")
	}

	if keyspace != "" {
		st, err := cn.Prepare(fmt.Sprintf("USE %s", keyspace))
		if err != nil {
			return nil, err
		}
		if _, err = st.Exec([]driver.Value{}); err != nil {
			return nil, err
		}
	}

	return cn, nil
}

// close a connection actively, typically used when there's an error and we want to ensure
// we don't repeatedly try to use the broken connection
func (cn *connection) close() {
	cn.c.Close()
	cn.c = nil // ensure we generate ErrBadConn when cn gets reused
}

func (cn *connection) send(opcode byte, body []byte) error {
	if cn.c == nil {
		return driver.ErrBadConn
	}
	frame := make([]byte, len(body)+8)
	frame[0] = protoRequest
	frame[1] = 0
	frame[2] = 0
	frame[3] = opcode
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(body)))
	copy(frame[8:], body)
	if _, err := cn.c.Write(frame); err != nil {
		return err
	}
	return nil
}

func (cn *connection) recv() (byte, []byte, error) {
	if cn.c == nil {
		return 0, nil, driver.ErrBadConn
	}
	header := make([]byte, 8)
	if _, err := io.ReadFull(cn.c, header); err != nil {
		cn.close() // better assume that the connection is broken (may have read some bytes)
		return 0, nil, err
	}
	// verify that the frame starts with version==1 and req/resp flag==response
	// this may be overly conservative in that future versions may be backwards compatible
	// in that case simply amend the check...
	if header[0] != protoResponse {
		cn.close()
		return 0, nil, fmt.Errorf("unsupported frame version or not a response: 0x%x (header=%v)", header[0], header)
	}
	// verify that the flags field has only a single flag set, again, this may
	// be overly conservative if additional flags are backwards-compatible
	if header[1] > 1 {
		cn.close()
		return 0, nil, fmt.Errorf("unsupported frame flags: 0x%x (header=%v)", header[1], header)
	}
	opcode := header[3]
	if opcode > opLAST {
		cn.close()
		return 0, nil, fmt.Errorf("unknown opcode: 0x%x (header=%v)", opcode, header)
	}
	length := binary.BigEndian.Uint32(header[4:8])
	var body []byte
	if length > 0 {
		if length > 256*1024*1024 { // spec says 256MB is max
			cn.close()
			return 0, nil, fmt.Errorf("frame too large: %d (header=%v)", length, header)
		}
		body = make([]byte, length)
		if _, err := io.ReadFull(cn.c, body); err != nil {
			cn.close() // better assume that the connection is broken
			return 0, nil, err
		}
	}
	if header[1]&flagCompressed != 0 && cn.compression == "snappy" {
		var err error
		body, err = snappy.Decode(nil, body)
		if err != nil {
			cn.close()
			return 0, nil, err
		}
	}
	if opcode == opError {
		code := binary.BigEndian.Uint32(body[0:4])
		msglen := binary.BigEndian.Uint16(body[4:6])
		msg := string(body[6 : 6+msglen])
		return opcode, body, Error{Code: int(code), Msg: msg}
	}
	return opcode, body, nil
}

func (cn *connection) Begin() (driver.Tx, error) {
	if cn.c == nil {
		return nil, driver.ErrBadConn
	}
	return cn, nil
}

func (cn *connection) Commit() error {
	if cn.c == nil {
		return driver.ErrBadConn
	}
	return nil
}

func (cn *connection) Close() error {
	if cn.c == nil {
		return driver.ErrBadConn
	}
	cn.close()
	return nil
}

func (cn *connection) Rollback() error {
	if cn.c == nil {
		return driver.ErrBadConn
	}
	return nil
}

func (cn *connection) Prepare(query string) (driver.Stmt, error) {
	body := make([]byte, len(query)+4)
	binary.BigEndian.PutUint32(body[0:4], uint32(len(query)))
	copy(body[4:], []byte(query))
	if err := cn.send(opPrepare, body); err != nil {
		return nil, err
	}
	opcode, body, err := cn.recv()
	if err != nil {
		return nil, err
	}
	if opcode != opResult || binary.BigEndian.Uint32(body) != 4 {
		return nil, fmt.Errorf("expected prepared result")
	}
	n := int(binary.BigEndian.Uint16(body[4:]))
	prepared := body[6 : 6+n]
	columns, meta, _ := parseMeta(body[6+n:])
	return &statement{cn: cn, query: query,
		prepared: prepared, columns: columns, meta: meta}, nil
}

type statement struct {
	cn       *connection
	query    string
	prepared []byte
	columns  []string
	meta     []uint16
}

func (s *statement) Close() error {
	return nil
}

func (st *statement) ColumnConverter(idx int) driver.ValueConverter {
	return (&columnEncoder{st.meta}).ColumnConverter(idx)
}

func (st *statement) NumInput() int {
	return len(st.columns)
}

func parseMeta(body []byte) ([]string, []uint16, int) {
	flags := binary.BigEndian.Uint32(body)
	globalTableSpec := flags&1 == 1
	columnCount := int(binary.BigEndian.Uint32(body[4:]))
	i := 8
	if globalTableSpec {
		l := int(binary.BigEndian.Uint16(body[i:]))
		keyspace := string(body[i+2 : i+2+l])
		i += 2 + l
		l = int(binary.BigEndian.Uint16(body[i:]))
		tablename := string(body[i+2 : i+2+l])
		i += 2 + l
		_, _ = keyspace, tablename
	}
	columns := make([]string, columnCount)
	meta := make([]uint16, columnCount)
	for c := 0; c < columnCount; c++ {
		l := int(binary.BigEndian.Uint16(body[i:]))
		columns[c] = string(body[i+2 : i+2+l])
		i += 2 + l
		meta[c] = binary.BigEndian.Uint16(body[i:])
		i += 2
	}
	return columns, meta, i
}

func (st *statement) exec(v []driver.Value) error {
	sz := 6 + len(st.prepared)
	for i := range v {
		if b, ok := v[i].([]byte); ok {
			sz += len(b) + 4
		}
	}
	body, p := make([]byte, sz), 4+len(st.prepared)
	binary.BigEndian.PutUint16(body, uint16(len(st.prepared)))
	copy(body[2:], st.prepared)
	binary.BigEndian.PutUint16(body[p-2:], uint16(len(v)))
	for i := range v {
		b, ok := v[i].([]byte)
		if !ok {
			return fmt.Errorf("unsupported type %T at column %d", v[i], i)
		}
		binary.BigEndian.PutUint32(body[p:], uint32(len(b)))
		copy(body[p+4:], b)
		p += 4 + len(b)
	}
	binary.BigEndian.PutUint16(body[p:], uint16(st.cn.consistency))
	if err := st.cn.send(opExecute, body); err != nil {
		return err
	}
	return nil
}

func (st *statement) Exec(v []driver.Value) (driver.Result, error) {
	if err := st.exec(v); err != nil {
		return nil, err
	}
	opcode, body, err := st.cn.recv()
	if err != nil {
		return nil, err
	}
	_, _ = opcode, body
	return nil, nil
}

func (st *statement) Query(v []driver.Value) (driver.Rows, error) {
	if err := st.exec(v); err != nil {
		return nil, err
	}
	opcode, body, err := st.cn.recv()
	if err != nil {
		return nil, err
	}
	kind := binary.BigEndian.Uint32(body[0:4])
	if opcode != opResult || kind != 2 {
		return nil, fmt.Errorf("expected rows as result")
	}
	columns, meta, n := parseMeta(body[4:])
	i := n + 4
	rows := &rows{
		columns: columns,
		meta:    meta,
		numRows: int(binary.BigEndian.Uint32(body[i:])),
	}
	i += 4
	rows.body = body[i:]
	return rows, nil
}

type rows struct {
	columns []string
	meta    []uint16
	body    []byte
	row     int
	numRows int
}

func (r *rows) Close() error {
	return nil
}

func (r *rows) Columns() []string {
	return r.columns
}

func (r *rows) Next(values []driver.Value) error {
	if r.row >= r.numRows {
		return io.EOF
	}
	for column := 0; column < len(r.columns); column++ {
		n := int32(binary.BigEndian.Uint32(r.body))
		r.body = r.body[4:]
		if n >= 0 {
			values[column] = decode(r.body[:n], r.meta[column])
			r.body = r.body[n:]
		} else {
			values[column] = nil
		}
	}
	r.row++
	return nil
}

type Error struct {
	Code int
	Msg  string
}

func (e Error) Error() string {
	return e.Msg
}

func init() {
	sql.Register("gocql", &drv{})
}
