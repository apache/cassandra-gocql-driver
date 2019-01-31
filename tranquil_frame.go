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
type FrameHeader frameHeader

func (hdr *FrameHeader) String() string {
	return ((*frameHeader)(hdr)).String()
}

// Returns the CQL frame operation code within the DQL frame header.
func (hdr *FrameHeader) Op() FrameOp {
	return FrameOp(((*frameHeader)(hdr)).op)
}

// Returns the stream ID within the CQL frame header.
func (hdr *FrameHeader) Stream() int {
	return ((*frameHeader)(hdr)).Header().stream
}

// Derive a request id from the CQL frame header.
func (hdr *FrameHeader) RequestId() uint64 {
	return uint64(hdr.Stream())
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

// Strcuture for handling the mechanics of all Cassandra frames
type CqlFramer struct {
	input        io.Reader
	output       io.Writer
	framer       *framer    // Framer for receiving and sending responses back to the client
	compressor   Compressor // CQL protocol compressor used for the client connection
	protoVersion byte
	frame        frame
}

func NewCqlFramer(r io.Reader, w io.Writer) *CqlFramer {
	return &CqlFramer{
		input:      r,
		output:     w,
		framer:     nil,
		compressor: nil,
	}
}

// Sets the protocol features such as compression and version
// based on the provided frame header information
func (cframer *CqlFramer) SetProtocolFeatures(hdr *FrameHeader) {
	if cframer.framer == nil {
		cframer.protoVersion = byte(hdr.version)
		cframer.framer = newFramer(cframer.input, cframer.output, cframer.compressor, cframer.protoVersion)
	}
}

// ReadHeader reads and returns the frame header.
func (cframer *CqlFramer) ReadHeader() (head FrameHeader, err error) {
	hdr, err := readHeader(cframer.input, make([]byte, 9))
	return FrameHeader(hdr), err
}

// ReadFrame reads the body of the frame with head and returns the parsed frame.
func (cframer *CqlFramer) ReadFrame(head *FrameHeader) (Frame, error) {
	var hdr = ((*frameHeader)(head))
	if cframer.framer == nil {
		fmt.Errorf("Framer is not setup")
	}
	var f = cframer.framer
	err := f.readFrame(hdr)
	if err != nil {
		return nil, err
	}
	frame, err := f.parseClientFrame()
	if err != nil {
		return nil, err
	}
	return Frame(frame), err
}

func (cframer *CqlFramer) getFramer(w io.Writer) *framer {
	if w == nil {
		return cframer.framer
	} else {
		return newFramer(nil, w, cframer.compressor, cframer.protoVersion)
	}
}

// WriteReadyFrame writes the READY frame with steam ID with the provided io.Writer.
func (cframer *CqlFramer) WriteReadyFrame(stream int, w io.Writer) error {
	return cframer.getFramer(w).writeReadyFrame(stream)
}

// WriteSupportedFrame writes the SUPPORTED frame with steam ID with the provided io.Writer.
func (cframer *CqlFramer) WriteSupportedFrame(stream int, w io.Writer) error {
	return cframer.getFramer(w).writeSupportedFrame(stream)
}

// WriteErrorFrame writes the ERROR frame with steam ID, error code, and message
// with the provided io.Writer.
func (cframer *CqlFramer) WriteErrorFrame(stream int, code int32, message string, w io.Writer) error {
	// Unauthorized: The logged user doesn't have the right to perform the query.
	// Invalid: The query is syntactically correct but invalid.
	return cframer.getFramer(w).writeErrorFrame(stream, code, message)
}

// WriteSetupFrame writes the SETUP rame with steam ID and version
// with the provided io.Writer.
func (cframer *CqlFramer) WriteSetupFrame(stream int, version uint8, w io.Writer) error {
	m := map[string]string{
		"CQL_VERSION": "3.0.0", // the only version supported
	}

	// Don't do any compression
	// m["COMPRESSION"] = c.compressor.Name()

	framer := newFramer(nil, w, nil, version)
	req := &writeStartupFrame{opts: m}

	return req.writeFrame(framer, stream)
}

// WriteQueryFrame generates the the QUERY frame bytes with the provided head
// and parsed frame.
func (cframer *CqlFramer) WriteQueryFrame(head *FrameHeader, qf Frame) ([]byte, error) {
	var hdr = ((*frameHeader)(head))
	var frame = frame(qf)
	b := new(bytes.Buffer)
	f := newFramer(nil, b, nil, cframer.protoVersion)
	qfw := &writeQueryFrame{
		statement: frame.(*queryFrame).statement,
		params:    frame.(*queryFrame).params,
	}
	if err := qfw.writeFrame(f, hdr.Header().stream); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// ReadFullFrameBytes reads the frame body for the given head and returns
// the full frame as a byte array.
func (cframer *CqlFramer) ReadFullFrameBytes(head *FrameHeader) ([]byte, error) {
	hdr := ((*frameHeader)(head))

	// Read additional data beyond the frame header, if any
	if hdr.length < 0 {
		return nil, fmt.Errorf("frame body length can not be less than 0: %d", hdr.length)
	} else if hdr.length > maxFrameSize {
		// need to free up the connection to be used again
		_, err := io.CopyN(ioutil.Discard, cframer.input, int64(head.length))
		if err != nil {
			return nil, fmt.Errorf("error whilst trying to discard frame with invalid length: %v", err)
		}
		return nil, ErrFrameTooBig
	}
	payload := make([]byte, hdr.length)

	// assume the underlying reader takes care of timeouts and retries
	n, err := io.ReadFull(cframer.input, payload)
	if err != nil {
		return nil, fmt.Errorf("unable to read frame body: read %d/%d bytes: %v", n, head.length, err)
	}

	return cframer.writeFullFrameBytes(hdr, payload)
}

// GetPayloadFromFullFrameBytes extracts the bytes from the slide b corresponding
// to the frame body.
func (cframer *CqlFramer) GetPayloadFromFullFrameBytes(head *FrameHeader, b []byte) []byte {
	hdr := ((*frameHeader)(head))
	headSize := 8
	if hdr.Header().version > protoVersion2 {
		headSize = 9
	}
	return b[headSize:]
}

// PullStatementFromFrame parses a frame containing CQL query statement(s) and
// returns a string representing the query and a flag if it is a prepared statement.
func (cframer *CqlFramer) PullStatementFromFrame(f Frame) (string, bool, error) {
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

// ReadResultFrame reads the frame body for the frame head from the connection
// associated with the CqlFramer and returns the parsed results.
// It also returns the frame bytes, and the list of columns returned.
func (cframer *CqlFramer) ReadResultFrame(head *FrameHeader) (cols []ColumnInfo,
	results []map[string]interface{}, frameBytes []byte, err error) {

	var hdr = ((*frameHeader)(head))
	f := cframer.framer
	if hdr.op != opResult {
		err = errors.New("Expected Result Frame")
		return
	}
	if err = f.readFrame(hdr); err != nil {
		return
	}

	if parsedFrame, perr := f.parseResultFrame(); perr != nil {
		err = perr
		return
	} else {
		if frame, ok := parsedFrame.(*resultRowsFrame); !ok {
			err = errors.New("Expected Rows kind of Result Frame")
			return
		} else {
			// Set the final result frame flag value, if needed
			// final = frame.meta.flags&flagHasMorePages == 0

			iter := Iter{
				meta:    frame.meta,
				numRows: frame.numRows,
				framer:  cframer.framer,
			}
			if results, err = iter.SliceMap(); err != nil {
				return
			}
			cols = frame.meta.columns

			// Write out the frame bytes, leverage the framer's buffer
			frameBytes, err = cframer.writeFullFrameBytes(hdr, f.readBuffer)
		}
	}
	return
}

func (cframer *CqlFramer) writeFullFrameBytes(hdr *frameHeader, payload []byte) ([]byte, error) {
	b := new(bytes.Buffer)
	f := newFramer(nil, b, nil, byte(hdr.version))
	f.writeHeader(hdr.flags, hdr.op, hdr.stream)
	// Use this low-level method on the framer to write the payload as raw bytes.
	// The existing f.writeBytes() method expands writes CQL protocol-encoded bytes.
	f.wbuf = append(f.wbuf, payload...)

	if err := f.finishWrite(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
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
			if b != nil {
				opts.values[i].isUnset = true
			} else {
				opts.values[i].isUnset = false
				opts.values[i].value = f.readBytes()
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
	statement string
}

func (f *framer) parsePrepareFrame() frame {
	return &prepareFrame{
		frameHeader: *f.header,
		statement:   f.readLongString(),
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
