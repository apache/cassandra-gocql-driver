/*
 * Copyright (c) 2018, Tranquil Data, Inc. All rights reserved.
 */

/*
 * This source file defines Cassandra CQL protocol frames.
 * The structs and constants re-define the gocql internal constructs, so that
 * they are exported to other packages. It also contains additional functions
 * and defintions complementing the gocql so that a Tranquil Data node can also
 * act as Cassandra server.
 *
 * All code contained herein is subject to the copyright above and is not part
 * of the gocql package. The file has to live in this particular location that
 * is within the gocql code so that it can be part of the same package and
 * access non-exported constructs.
 */
package gocql

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"runtime"
)

// CQL frame operation
type FrameOp frameOp

// Returns string representation of the CQL frame operation.
func (f FrameOp) String() string {
	return frameOp(f).String()
}

// CQL frame header
// type FrameHeader frameHeader
type FrameHeader struct {
	hdr      frameHeader
	hdrBytes []byte
}

func (hdr *FrameHeader) String() string {
	return hdr.hdr.String()
}

// Returns the CQL frame operation code within the DQL frame header.
func (hdr *FrameHeader) Op() FrameOp {
	return FrameOp(hdr.hdr.op)
}

// Returns the stream ID within the CQL frame header.
func (hdr *FrameHeader) Stream() int {
	return (hdr.hdr.Header().stream)
}

type Frame frame

const (
	// header ops
	OpError         FrameOp = FrameOp(opError)
	OpStartup       FrameOp = FrameOp(opStartup)
	OpReady         FrameOp = FrameOp(opReady)
	OpAuthenticate  FrameOp = FrameOp(opAuthenticate)
	OpOptions       FrameOp = FrameOp(opOptions)
	OpSupported     FrameOp = FrameOp(opSupported)
	OpQuery         FrameOp = FrameOp(opQuery)
	OpResult        FrameOp = FrameOp(opResult)
	OpPrepare       FrameOp = FrameOp(opPrepare)
	OpExecute       FrameOp = FrameOp(opExecute)
	OpRegister      FrameOp = FrameOp(opRegister)
	OpEvent         FrameOp = FrameOp(opEvent)
	OpBatch         FrameOp = FrameOp(opBatch)
	OpAuthChallenge FrameOp = FrameOp(opAuthChallenge)
	OpAuthResponse  FrameOp = FrameOp(opAuthResponse)
	OpAuthSuccess   FrameOp = FrameOp(opAuthSuccess)
)

const (
	ErrUnauthorized int32 = errUnauthorized
	ErrInvalid      int32 = errInvalid
)

// Framer handles the mechanics of manipulating Cassandra frames.
type Framer struct {
	input        io.Reader
	output       io.Writer
	framer       *framer    // Framer for receiving and sending responses back to the client
	compressor   Compressor // CQL protocol compressor used for the client connection
	protoVersion byte
}

func NewFramer(r io.Reader, w io.Writer, version byte) *Framer {
	return &Framer{
		input:      r,
		output:     w,
		framer:     nil,
		compressor: nil,
	}
}

// SetProtocolFeatures sets the protocol features such as compression and
// version based on the provided frame header information
func (framer *Framer) SetProtocolFeatures(hdr *FrameHeader) {
	if framer.framer == nil {
		framer.protoVersion = byte(hdr.hdr.version)
		framer.framer = newFramer(framer.input, framer.output, framer.compressor, framer.protoVersion)
	}
}

// ReadHeader reads and returns the frame header.
func (framer *Framer) ReadHeader() (head FrameHeader, err error) {
	head.hdrBytes = make([]byte, 9)
	head.hdr, err = readHeader(framer.input, head.hdrBytes)
	return head, err
}

// ReadFrame reads the body of the frame with head and returns the parsed frame
// and the full frame, which includes the header, as a byte array.
func (framer *Framer) ReadFrame(head *FrameHeader) (Frame, []byte, error) {
	if framer.framer == nil {
		return nil, nil, fmt.Errorf("Framer is not setup")
	}
	var f = framer.framer
	err := f.readFrame(&head.hdr)
	if err != nil {
		return nil, nil, err
	}
	frame, err := f.parseClientFrame()
	if err != nil {
		return nil, nil, err
	}

	frameBytes := framer.fullFrameBytes(head)
	return Frame(frame), frameBytes, nil
}

func (framer *Framer) getFramer(w io.Writer) *framer {
	if w == nil {
		return framer.framer
	} else {
		return newFramer(nil, w, framer.compressor, framer.protoVersion)
	}
}

// WriteReadyFrame writes the READY frame with steam ID with the provided io.Writer.
func (framer *Framer) WriteReadyFrame(stream int, w io.Writer) error {
	return framer.getFramer(w).writeReadyFrame(stream)
}

// WriteSupportedFrame writes the SUPPORTED frame with steam ID with the provided io.Writer.
func (framer *Framer) WriteSupportedFrame(stream int, w io.Writer) error {
	return framer.getFramer(w).writeSupportedFrame(stream)
}

// WriteErrorFrame writes the ERROR frame with steam ID, error code, and message
// with the provided io.Writer.
func (framer *Framer) WriteErrorFrame(stream int, code int32, message string, w io.Writer) error {
	// Unauthorized: The logged user doesn't have the right to perform the query.
	// Invalid: The query is syntactically correct but invalid.
	return framer.getFramer(w).writeErrorFrame(stream, code, message)
}

// WriteSetupFrame writes the SETUP rame with steam ID and version
// with the provided io.Writer.
func (framer *Framer) WriteSetupFrame(stream int, version uint8, w io.Writer) error {
	m := map[string]string{
		"CQL_VERSION": "3.0.0", // the only version supported
	}

	// Don't do any compression
	// m["COMPRESSION"] = c.compressor.Name()

	f := newFramer(nil, w, nil, version)
	req := &writeStartupFrame{opts: m}

	return req.writeFrame(f, stream)
}

// WriteQueryFrame generates QUERY frame bytes with the provided head
// and parsed frame.
func (framer *Framer) WriteQueryFrame(head *FrameHeader, qf Frame) ([]byte, error) {
	var frame = frame(qf)
	b := new(bytes.Buffer)
	f := newFramer(nil, b, nil, framer.protoVersion)
	qfw := &writeQueryFrame{
		statement: frame.(*queryFrame).statement,
		params:    frame.(*queryFrame).params,
	}
	if err := qfw.writeFrame(f, head.hdr.Header().stream); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// WritePrepareFrame generates PREPARE frame bytes with the provided head
// and parsed frame.
func (framer *Framer) WritePrepareFrame(head *FrameHeader, qf Frame) ([]byte, error) {
	var frame = frame(qf)
	b := new(bytes.Buffer)
	f := newFramer(nil, b, nil, framer.protoVersion)
	qfw := &writePrepareFrame{
		statement:     frame.(*prepareFrame).statement,
		keyspace:      frame.(*prepareFrame).keyspace,
		customPayload: frame.(*prepareFrame).customPayload,
	}
	if err := qfw.writeFrame(f, head.hdr.Header().stream); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// WriteExecuteFrame generates EXECUTE frame bytes with the provided head
// and parsed frame.
func (framer *Framer) WriteExecuteFrame(head *FrameHeader, qf Frame) ([]byte, error) {
	var frame = frame(qf)
	b := new(bytes.Buffer)
	f := newFramer(nil, b, nil, framer.protoVersion)
	qfw := &writeExecuteFrame{
		preparedID:    frame.(*executeFrame).preparedID,
		params:        frame.(*executeFrame).params,
		customPayload: frame.(*executeFrame).customPayload,
	}
	if err := qfw.writeFrame(f, head.hdr.Header().stream); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// ReadFrameBytes reads the frame body for the given head and returns
// the full frame as a byte array.
func (framer *Framer) ReadFrameBytes(head *FrameHeader) ([]byte, error) {
	hdr := head.hdr

	// Read additional data beyond the frame header, if any
	if hdr.length < 0 {
		return nil, fmt.Errorf("frame body length can not be less than 0: %d", hdr.length)
	} else if hdr.length > maxFrameSize {
		// need to free up the connection to be used again
		_, err := io.CopyN(ioutil.Discard, framer.input, int64(hdr.length))
		if err != nil {
			return nil, fmt.Errorf("error whilst trying to discard frame with invalid length: %v", err)
		}
		return nil, ErrFrameTooBig
	}

	hlen := len(head.hdrBytes)
	fullFrameBytes := make([]byte, hlen+hdr.length)
	copy(fullFrameBytes, head.hdrBytes)

	// assume the underlying reader takes care of timeouts and retries
	n, err := io.ReadFull(framer.input, fullFrameBytes[hlen:])
	if err != nil {
		return nil, fmt.Errorf("unable to read frame body: read %d/%d bytes: %v", n, hdr.length, err)
	}
	return fullFrameBytes, nil
}

// PullStatementFromFrame parses a frame containing CQL query statement(s) and
// returns a string representing the query and a flag if it is a prepared statement.
func (framer *Framer) PullStatementFromFrame(f Frame) (string, bool, error) {
	var frame = frame(f)
	if f, ok := frame.(*queryFrame); ok {
		return f.statement, false, nil
	}
	if f, ok := frame.(*prepareFrame); ok {
		return f.statement, true, nil
	}
	if _, ok := frame.(*batchFrame); ok {
		panic("BATCH frame not yet supported")
	}
	return "", false, errors.New("Unknown frame")
}

// PullFromExecuteFrame returns prepared ID and parameters from the EXECUTE frame.
func (framer *Framer) PullFromExecuteFrame(f Frame) ([]byte, QueryParams, error) {
	var frame = frame(f)
	if f, ok := frame.(*executeFrame); ok {

		values := make([]QueryValues, 0, len(f.params.values))
		for _, v := range f.params.values {
			values = append(values, QueryValues{
				Value:   v.value,
				Name:    v.name,
				IsUnset: v.isUnset,
			})
		}
		return f.preparedID, QueryParams{Values: values}, nil
	}
	return nil, QueryParams{}, errors.New("Unknown frame")
}

// ReadResultFrame reads the frame body for the frame head from the associated
// reader and returns the parsed result. It also returns the full frame bytes.
func (framer *Framer) ReadResultFrame(head *FrameHeader, ci []ColumnInfo) (result interface{},
	frameBytes []byte, err error) {

	hdr := head.hdr
	f := framer.framer
	if hdr.op != opResult {
		err = errors.New("Expected Result Frame")
		return
	}
	if err = f.readFrame(&hdr); err != nil {
		return
	}
	parsedFrame, perr := f.parseResultFrame()
	if perr != nil {
		err = perr
		return
	}
	switch frame := parsedFrame.(type) {
	default:
		// Includes these possible types
		// (*resultVoidFrame)
		// (*schemaChangeKeyspace), (*schemaChangeTable), (*schemaChangeType),
		// (*schemaChangeFunction), (*schemaChangeAggregate)
		result = nil
	case (*resultKeyspaceFrame):
		// We don't do anything now with USE KEYSPACE response
		result = nil
	case (*resultPreparedFrame):
		r := PreparedResult{
			ID:      frame.preparedID,
			Columns: frame.respMeta.columns,
		}
		result = r
	case (*resultRowsFrame):
		meta := frame.meta
		columns := frame.meta.columns
		if ci != nil {
			// Swap in column info from the earlier PREPARE result
			columns = ci
			meta.columns = ci
		}
		iter := Iter{
			meta:    meta,
			numRows: frame.numRows,
			framer:  framer.framer,
		}
		r := RowsResult{
			Cols:             columns,
			FlagHasMorePages: frame.meta.flags&flagHasMorePages == 1,
		}
		if r.Rows, err = iter.SliceMap(); err != nil {
			return
		}
		result = r
	}

	frameBytes = framer.fullFrameBytes(head)

	return
}

// fullFrameBytes returns a new byte slice containing the frame head and body.
// The body is extracted from the framer's internal buffers.
func (framer *Framer) fullFrameBytes(head *FrameHeader) []byte {
	hlen := len(head.hdrBytes)
	payload := framer.framer.readBuffer
	fullFrameBytes := make([]byte, hlen+head.hdr.length)
	copy(fullFrameBytes, head.hdrBytes)
	copy(fullFrameBytes[hlen:], payload)
	return fullFrameBytes
}

/******************************************************************************
 * The gocql is a client library. Therefore it does not handle all CQL frames
 * defined by the CQL binary protocol. The following are server-side-related
 * functions, types, and structs. They complement those in the gocql/frame.go.
 ******************************************************************************/
func (f *framer) parseClientFrame() (frame frame, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
		}
	}()

	if !f.header.version.request() {
		return nil, NewErrProtocol("got a request frame from server: %v", f.header.version)
	}

	if f.header.flags&flagTracing == flagTracing {
		f.readTrace()
	}

	if f.header.flags&flagWarning == flagWarning {
		f.header.warnings = f.readStringList()
	}

	if f.header.flags&flagCustomPayload == flagCustomPayload {
		f.customPayload = f.readBytesMap()
	}

	// assumes that the frame body has been read into rbuf
	switch f.header.op {
	case opStartup:
		frame = f.parseStartupFrame()
	case opOptions:
		frame = f.parseOptionsFrame()
	case opRegister:
		frame = f.parseRegisterFrame()
	case opQuery:
		frame = f.parseQueryFrame()
	case opPrepare:
		frame = f.parsePrepareFrame()
	case opExecute:
		frame = f.parseExecuteFrame()
	case opBatch:
		frame = f.parseBatchFrame()
	default:
		return nil, NewErrProtocol("unknown op in frame header: %s", f.header.op)
	}

	return
}

type startupFrame struct {
	frameHeader
	startup map[string]string
}

func (f *framer) parseStartupFrame() frame {
	return &startupFrame{
		frameHeader: *f.header,
		startup:     f.readStringMap(),
	}
}

type registerFrame struct {
	frameHeader
	register []string
}

func (f *framer) parseRegisterFrame() frame {
	return &registerFrame{
		frameHeader: *f.header,
		register:    f.readStringList(),
	}
}

type optionsFrame struct {
	frameHeader
	// No payload
}

func (f *framer) parseOptionsFrame() frame {
	return &optionsFrame{
		frameHeader: *f.header,
	}
}

// QueryParams defines parameters for EXECUTE frame
type QueryParams struct {
	Values []QueryValues
}

// QueryValues defines a value provided in the EXECUTE frame
type QueryValues struct {
	Value []byte

	// optional name, will set With names for values flag
	Name    string
	IsUnset bool
}

type queryFrame struct {
	frameHeader
	statement string
	params    queryParams
}

func (f *framer) parseQueryFrame() frame {
	return &queryFrame{
		frameHeader: *f.header,
		statement:   f.readLongString(),
		params:      f.readQueryParams(),
	}
}

func (f *framer) readQueryParams() (opts queryParams) {
	opts = queryParams{
		consistency: f.readConsistency(),
	}
	if f.header.version < protoVersion2 {
		return opts
	}
	flags := f.readByte()

	if (flags & flagSkipMetaData) != 0 {
		opts.skipMeta = true
	}

	if (flags & flagValues) != 0 {
		n := int(f.readShort())
		opts.values = make([]queryValues, n)
		for i := 0; i < n; i++ {
			if (flags & flagWithNameValues) != 0 {
				opts.values[i].name = f.readString()
			}
			b := f.readBytes()
			if b == nil {
				opts.values[i].isUnset = true
			} else {
				opts.values[i].isUnset = false
				opts.values[i].value = b
			}
		}
	}

	if (flags & flagPageSize) != 0 {
		opts.pageSize = f.readInt()
	}

	if (flags & flagWithPagingState) != 0 {
		opts.pagingState = f.readBytes()
	}

	if (flags & flagWithSerialConsistency) != 0 {
		opts.serialConsistency = SerialConsistency(f.readConsistency())
	}

	if f.proto > protoVersion2 {
		if (flags & flagDefaultTimestamp) != 0 {
			opts.defaultTimestamp = true
			opts.defaultTimestampValue = f.readLong()
		}
	}

	return opts
}

type prepareFrame struct {
	frameHeader
	statement     string
	keyspace      string
	customPayload map[string][]byte
}

func (f *framer) parsePrepareFrame() frame {
	return &prepareFrame{
		frameHeader: *f.header,
		statement:   f.readLongString(),
	}
}

type executeFrame struct {
	frameHeader
	preparedID []byte
	params     queryParams

	// v4+
	customPayload map[string][]byte
}

func (f *framer) parseExecuteFrame() frame {
	return &executeFrame{
		frameHeader: *f.header,
		preparedID:  f.readShortBytes(),
		params:      f.readQueryParams(),
	}
}

type batchFrame struct {
	frameHeader
	typ         BatchType
	statements  []batchStatment
	consistency Consistency

	// v3+
	serialConsistency     SerialConsistency
	defaultTimestamp      bool
	defaultTimestampValue int64
}

func (f *framer) parseBatchFrame() frame {

	bf := &batchFrame{
		frameHeader: *f.header,
		typ:         BatchType(f.readByte()),
		// Other values are filled in below
	}

	n := int(f.readShort())
	bf.statements = make([]batchStatment, n)

	// FIXME: Flags is really after the list of queries, not the frame ones
	var flags byte = f.flags

	for i := 0; i < int(n); i++ {
		kind := f.readByte()
		if kind == 0 {
			bf.statements[i].statement = f.readLongString()
		} else {
			bf.statements[i].preparedID = f.readShortBytes()
		}

		opts := bf.statements[i]
		if (flags & flagValues) != 0 {
			cnt := int(f.readShort())
			opts.values = make([]queryValues, cnt)
			for j := 0; j < cnt; j++ {
				if (flags & flagWithNameValues) != 0 {
					opts.values[j].name = f.readString()
				}
				b := f.readBytes()
				if b != nil {
					opts.values[j].isUnset = true
				} else {
					opts.values[j].isUnset = false
					opts.values[j].value = f.readBytes()
				}
			}
		}
	}

	bf.consistency = f.readConsistency()

	// Read flags
	flags = f.readByte()

	if f.proto > protoVersion2 {
		if (flags & flagWithSerialConsistency) != 0 {
			bf.serialConsistency = SerialConsistency(f.readConsistency())
		}
		if (flags & flagDefaultTimestamp) != 0 {
			bf.defaultTimestamp = true
			bf.defaultTimestampValue = f.readLong()
		}
	}

	return bf
}

/***************** Write Frame helper functions **************/

func (f *framer) writeReadyFrame(streamID int) error {
	f.writeHeader(f.flags, opReady, streamID)
	// explicitly set the response direction/flag
	f.wbuf[0] |= protoDirectionMask
	return f.finishWrite()
}

func (f *framer) writeSupportedFrame(streamID int) error {
	f.writeHeader(f.flags, opSupported, streamID)
	// explicitly set the response direction/flag
	f.wbuf[0] |= protoDirectionMask
	return f.finishWrite()
}

func (f *framer) writeErrorFrame(streamID int, code int32, message string) error {
	f.writeHeader(f.flags, opError, streamID)
	// explicitly set the response direction/flag
	f.wbuf[0] |= protoDirectionMask

	f.writeInt(code)
	f.writeString(message)

	return f.finishWrite()
}

// RowsResult represents the ResultFrame payload from Rows "kind".
type RowsResult struct {
	Cols             []ColumnInfo
	Rows             []map[string]interface{}
	FlagHasMorePages bool
}

// PreparedResult represents the ResultFrame payload from Prepared "kind".
type PreparedResult struct {
	ID      []byte
	Columns []ColumnInfo
}

// CassandraType returns the type information for the Cassandra type string
// representation such as the one returned when querying system tables.
func CassandraType(name string) TypeInfo {
	// OK to use a reasonably recent protocol version. We want to cover a large
	// set of possible types returned; protocols are backward compatible.
	p := byte(protoVersion4)
	return NativeType{proto: p, typ: getCassandraBaseType(name)}
}
