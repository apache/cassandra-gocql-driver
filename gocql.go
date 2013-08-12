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
	"net"
	"strings"
	"time"
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
	keyspaceQuery  string = "USE "
)

var consistencyLevels = map[string]byte{"any": 0x00, "one": 0x01, "two": 0x02,
	"three": 0x03, "quorum": 0x04, "all": 0x05, "local_quorum": 0x06, "each_quorum": 0x07}

type drv struct{}

func (d drv) Open(name string) (driver.Conn, error) {
	return Open(name)
}

type connection struct {
	c       net.Conn
	address string
	alive   bool
	pool    *pool
}

type pool struct {
	connections []*connection
	i           int
	keyspace    string
	version     string
	compression string
	consistency byte
	dead        bool
	stop        chan struct{}
}

func Open(name string) (*pool, error) {
	parts := strings.Split(name, " ")
	var addresses []string
	if len(parts) >= 1 {
		addresses = strings.Split(parts[0], ",")
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

	pool := &pool{
		keyspace:    keyspace,
		version:     version,
		compression: compression,
		consistency: consistency,
		stop:        make(chan struct{}),
	}

	for _, address := range addresses {
		pool.connections = append(pool.connections, &connection{address: address, pool: pool})
	}

	pool.join()

	return pool, nil
}

func (cn *connection) open() {
	cn.alive = false

	var err error
	cn.c, err = net.Dial("tcp", cn.address)
	if err != nil {
		return
	}

	var (
		version     = cn.pool.version
		compression = cn.pool.compression
		keyspace    = cn.pool.keyspace
	)

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

	if err := cn.sendUncompressed(opStartup, b.Bytes()); err != nil {
		return
	}

	opcode, _, err := cn.recv()
	if err != nil {
		return
	}
	if opcode != opReady {
		return
	}

	if keyspace != "" {
		cn.UseKeyspace(keyspace)
	}

	cn.alive = true
}

// close a connection actively, typically used when there's an error and we want to ensure
// we don't repeatedly try to use the broken connection
func (cn *connection) close() {
	cn.c.Close()
	cn.c = nil // ensure we generate ErrBadConn when cn gets reused
	cn.alive = false

	// Check if the entire pool is dead
	for _, cn := range cn.pool.connections {
		if cn.alive {
			return
		}
	}
	cn.pool.dead = false
}

// explicitly send a request as uncompressed
// This is only really needed for the "startup" handshake
func (cn *connection) sendUncompressed(opcode byte, body []byte) error {
	return cn._send(opcode, body, false)
}

func (cn *connection) send(opcode byte, body []byte) error {
	return cn._send(opcode, body, cn.pool.compression == "snappy" && len(body) > 0)
}

func (cn *connection) _send(opcode byte, body []byte, compression bool) error {
	if cn.c == nil {
		return driver.ErrBadConn
	}
	var flags byte = 0x00
	if compression && len(body) > 1 {
		var err error
		body, err = snappy.Encode(nil, body)
		if err != nil {
			return err
		}
		flags = flagCompressed
	}
	frame := make([]byte, len(body)+8)
	frame[0] = protoRequest
	frame[1] = flags
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
	if header[1]&flagCompressed != 0 && cn.pool.compression == "snappy" {
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

func (p *pool) conn() (*connection, error) {
	if p.dead {
		return nil, driver.ErrBadConn
	}

	totalConnections := len(p.connections)
	start := p.i + 1 // make sure that we start from the next position in the ring

	for i := 0; i < totalConnections; i++ {
		idx := (i + start) % totalConnections
		cn := p.connections[idx]
		if cn.alive {
			p.i = idx // set the new 'i' so the ring will start again in the right place
			return cn, nil
		}
	}

	// we've exhausted the pool, gonna have a bad time
	p.dead = true
	return nil, driver.ErrBadConn
}

func (p *pool) join() {
	p.reconnect()

	// Every 1 second, we want to try reconnecting to disconnected nodes
	go func() {
		for {
			select {
			case <-p.stop:
				return
			default:
				p.reconnect()
				time.Sleep(time.Second)
			}
		}
	}()
}

func (p *pool) reconnect() {
	for _, cn := range p.connections {
		if !cn.alive {
			cn.open()
		}
	}
}

func (p *pool) Begin() (driver.Tx, error) {
	if p.dead {
		return nil, driver.ErrBadConn
	}
	return p, nil
}

func (p *pool) Commit() error {
	if p.dead {
		return driver.ErrBadConn
	}
	return nil
}

func (p *pool) Close() error {
	if p.dead {
		return driver.ErrBadConn
	}
	for _, cn := range p.connections {
		cn.close()
	}
	p.stop <- struct{}{}
	p.dead = true
	return nil
}

func (p *pool) Rollback() error {
	if p.dead {
		return driver.ErrBadConn
	}
	return nil
}

func (p *pool) Prepare(query string) (driver.Stmt, error) {
	// Explicitly check if the query is a "USE <keyspace>"
	// Since it needs to be special cased and run on each server
	if strings.HasPrefix(query, keyspaceQuery) {
		keyspace := query[len(keyspaceQuery):]
		p.UseKeyspace(keyspace)
		return &statement{}, nil
	}

	for {
		cn, err := p.conn()
		if err != nil {
			return nil, err
		}
		st, err := cn.Prepare(query)
		if err == io.EOF {
			// the cn has gotten marked as dead already
			if p.dead {
				// The entire pool is dead, so we bubble up the ErrBadConn
				return nil, driver.ErrBadConn
			} else {
				continue // Retry request on another cn
			}
		}
		return st, err
	}
}

func (p *pool) UseKeyspace(keyspace string) {
	p.keyspace = keyspace
	for _, cn := range p.connections {
		cn.UseKeyspace(keyspace)
	}
}

func (cn *connection) UseKeyspace(keyspace string) error {
	st, err := cn.Prepare(keyspaceQuery + keyspace)
	if err != nil {
		return err
	}
	if _, err = st.Exec([]driver.Value{}); err != nil {
		return err
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
	binary.BigEndian.PutUint16(body[p:], uint16(st.cn.pool.consistency))
	if err := st.cn.send(opExecute, body); err != nil {
		return err
	}
	return nil
}

func (st *statement) Exec(v []driver.Value) (driver.Result, error) {
	if st.cn == nil {
		return nil, nil
	}
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
