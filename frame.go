// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const (
	protoDirectionMask = 0x80
	protoVersionMask   = 0x7F
	protoVersion1      = 0x01
	protoVersion2      = 0x02
	protoVersion3      = 0x03
)

type protoVersion byte

func (p protoVersion) request() bool {
	return p&protoDirectionMask == 0x00
}

func (p protoVersion) response() bool {
	return p&protoDirectionMask == 0x80
}

func (p protoVersion) version() byte {
	return byte(p) & protoVersionMask
}

func (p protoVersion) String() string {
	dir := "REQ"
	if p.response() {
		dir = "RESP"
	}

	return fmt.Sprintf("[version=%d direction=%s]", p.version(), dir)
}

type frameOp byte

const (
	// header ops
	opError         frameOp = 0x00
	opStartup               = 0x01
	opReady                 = 0x02
	opAuthenticate          = 0x03
	opOptions               = 0x05
	opSupported             = 0x06
	opQuery                 = 0x07
	opResult                = 0x08
	opPrepare               = 0x09
	opExecute               = 0x0A
	opRegister              = 0x0B
	opEvent                 = 0x0C
	opBatch                 = 0x0D
	opAuthChallenge         = 0x0E
	opAuthResponse          = 0x0F
	opAuthSuccess           = 0x10
)

func (f frameOp) String() string {
	switch f {
	case opError:
		return "ERROR"
	case opStartup:
		return "STARTUP"
	case opReady:
		return "READY"
	case opAuthenticate:
		return "AUTHENTICATE"
	case opOptions:
		return "OPTIONS"
	case opSupported:
		return "SUPPORTED"
	case opQuery:
		return "QUERY"
	case opResult:
		return "RESULT"
	case opPrepare:
		return "PREPARE"
	case opExecute:
		return "EXECUTE"
	case opRegister:
		return "REGISTER"
	case opEvent:
		return "EVENT"
	case opBatch:
		return "BATCH"
	case opAuthChallenge:
		return "AUTH_CHALLENGE"
	case opAuthResponse:
		return "AUTH_RESPONSE"
	case opAuthSuccess:
		return "AUTH_SUCCESS"
	default:
		return fmt.Sprintf("UNKNOWN_OP_%d", f)
	}
}

const (
	// result kind
	resultKindVoid          = 1
	resultKindRows          = 2
	resultKindKeyspace      = 3
	resultKindPrepared      = 4
	resultKindSchemaChanged = 5

	// rows flags
	flagGlobalTableSpec int = 0x01
	flagHasMorePages        = 0x02
	flagNoMetaData          = 0x04

	// query flags
	flagValues                byte = 0x01
	flagSkipMetaData               = 0x02
	flagPageSize                   = 0x04
	flagWithPagingState            = 0x08
	flagWithSerialConsistency      = 0x10
	flagDefaultTimestamp           = 0x20
	flagWithNameValues             = 0x40

	// header flags
	flagCompress byte = 0x01
	flagTracing       = 0x02
)

type Consistency uint16

const (
	Any         Consistency = 0x00
	One                     = 0x01
	Two                     = 0x02
	Three                   = 0x03
	Quorum                  = 0x04
	All                     = 0x05
	LocalQuorum             = 0x06
	EachQuorum              = 0x07
	Serial                  = 0x08
	LocalSerial             = 0x09
	LocalOne                = 0x0A
)

func (c Consistency) String() string {
	switch c {
	case Any:
		return "ANY"
	case One:
		return "ONE"
	case Two:
		return "TWO"
	case Three:
		return "THREE"
	case Quorum:
		return "QUORUM"
	case All:
		return "ALL"
	case LocalQuorum:
		return "LOCAL_QUORUM"
	case EachQuorum:
		return "EACH_QUORUM"
	case Serial:
		return "SERIAL"
	case LocalSerial:
		return "LOCAL_SERIAL"
	case LocalOne:
		return "LOCAL_ONE"
	default:
		return fmt.Sprintf("UNKNOWN_CONS_0x%x", uint16(c))
	}
}

const (
	apacheCassandraTypePrefix = "org.apache.cassandra.db.marshal."
)

func writeInt(p []byte, n int32) {
	p[0] = byte(n >> 24)
	p[1] = byte(n >> 16)
	p[2] = byte(n >> 8)
	p[3] = byte(n)
}

func readInt(p []byte) int32 {
	return int32(p[0])<<24 | int32(p[1])<<16 | int32(p[2])<<8 | int32(p[3])
}

func writeShort(p []byte, n uint16) {
	p[0] = byte(n >> 8)
	p[1] = byte(n)
}

func readShort(p []byte) uint16 {
	return uint16(p[0])<<8 | uint16(p[1])
}

type frameHeader struct {
	version protoVersion
	flags   byte
	stream  int
	op      frameOp
	length  int
}

func (f frameHeader) String() string {
	return fmt.Sprintf("[header version=%s flags=0x%x stream=%d op=%s length=%d]", f.version, f.flags, f.stream, f.op, f.length)
}

func (f frameHeader) Header() frameHeader {
	return f
}

const defaultBufSize = 128

var framerPool = sync.Pool{
	New: func() interface{} {
		return &framer{
			wbuf:       make([]byte, defaultBufSize),
			readBuffer: make([]byte, defaultBufSize),
		}
	},
}

// a framer is responsible for reading, writing and parsing frames on a single stream
type framer struct {
	r io.Reader
	w io.Writer

	proto byte
	// flags are for outgoing flags, enabling compression and tracing etc
	flags    byte
	compres  Compressor
	headSize int
	// if this frame was read then the header will be here
	header *frameHeader

	// if tracing flag is set this is not nil
	traceID []byte

	// holds a ref to the whole byte slice for rbuf so that it can be reset to
	// 0 after a read.
	readBuffer []byte

	rbuf []byte
	wbuf []byte
}

func newFramer(r io.Reader, w io.Writer, compressor Compressor, version byte) *framer {
	f := framerPool.Get().(*framer)
	var flags byte
	if compressor != nil {
		flags |= flagCompress
	}

	version &= protoVersionMask

	headSize := 8
	if version > protoVersion2 {
		headSize = 9
	}

	f.compres = compressor
	f.proto = version
	f.flags = flags
	f.headSize = headSize

	f.r = r
	f.rbuf = f.readBuffer[:0]

	f.w = w
	f.wbuf = f.wbuf[:0]

	f.header = nil
	f.traceID = nil

	return f
}

type frame interface {
	Header() frameHeader
}

func readHeader(r io.Reader, p []byte) (head frameHeader, err error) {
	_, err = io.ReadFull(r, p)
	if err != nil {
		return
	}

	version := p[0] & protoVersionMask
	head.version = protoVersion(p[0])

	head.flags = p[1]
	if version > protoVersion2 {
		head.stream = int(readShort(p[2:]))
		head.op = frameOp(p[4])
		head.length = int(readInt(p[5:]))
	} else {
		head.stream = int(p[2])
		head.op = frameOp(p[3])
		head.length = int(readInt(p[4:]))
	}

	return
}

// explicitly enables tracing for the framers outgoing requests
func (f *framer) trace() {
	f.flags |= flagTracing
}

// reads a frame form the wire into the framers buffer
func (f *framer) readFrame(head *frameHeader) error {
	if cap(f.readBuffer) >= head.length {
		f.rbuf = f.readBuffer[:head.length]
	} else {
		f.readBuffer = make([]byte, head.length)
		f.rbuf = f.readBuffer
	}

	// assume the underlying reader takes care of timeouts and retries
	_, err := io.ReadFull(f.r, f.rbuf)
	if err != nil {
		return err
	}

	if head.flags&flagCompress == flagCompress {
		if f.compres == nil {
			return NewErrProtocol("no compressor available with compressed frame body")
		}

		f.rbuf, err = f.compres.Decode(f.rbuf)
		if err != nil {
			return err
		}
	}

	f.header = head
	return nil
}

func (f *framer) parseFrame() (frame, error) {
	if f.header.version.request() {
		return nil, NewErrProtocol("got a request frame from server: %v", f.header.version)
	}

	if f.header.flags&flagTracing == flagTracing {
		f.readTrace()
	}

	var (
		frame frame
		err   error
	)

	// asumes that the frame body has been read into rbuf
	switch f.header.op {
	case opError:
		frame = f.parseErrorFrame()
	case opReady:
		frame = f.parseReadyFrame()
	case opResult:
		frame, err = f.parseResultFrame()
	case opSupported:
		frame = f.parseSupportedFrame()
	case opAuthenticate:
		frame = f.parseAuthenticateFrame()
	case opAuthChallenge:
		frame = f.parseAuthChallengeFrame()
	case opAuthSuccess:
		frame = f.parseAuthSuccessFrame()
	default:
		return nil, NewErrProtocol("unknown op in frame header: %s", f.header.op)
	}

	return frame, err
}

func (f *framer) parseErrorFrame() frame {
	code := f.readInt()
	msg := f.readString()

	errD := errorFrame{
		frameHeader: *f.header,
		code:        code,
		message:     msg,
	}

	switch code {
	case errUnavailable:
		cl := f.readConsistency()
		required := f.readInt()
		alive := f.readInt()
		return &RequestErrUnavailable{
			errorFrame:  errD,
			Consistency: cl,
			Required:    required,
			Alive:       alive,
		}
	case errWriteTimeout:
		cl := f.readConsistency()
		received := f.readInt()
		blockfor := f.readInt()
		writeType := f.readString()
		return &RequestErrWriteTimeout{
			errorFrame:  errD,
			Consistency: cl,
			Received:    received,
			BlockFor:    blockfor,
			WriteType:   writeType,
		}
	case errReadTimeout:
		cl := f.readConsistency()
		received := f.readInt()
		blockfor := f.readInt()
		dataPresent := f.readByte()
		return &RequestErrReadTimeout{
			errorFrame:  errD,
			Consistency: cl,
			Received:    received,
			BlockFor:    blockfor,
			DataPresent: dataPresent,
		}
	case errAlreadyExists:
		ks := f.readString()
		table := f.readString()
		return &RequestErrAlreadyExists{
			errorFrame: errD,
			Keyspace:   ks,
			Table:      table,
		}
	case errUnprepared:
		stmtId := f.readShortBytes()
		return &RequestErrUnprepared{
			errorFrame:  errD,
			StatementId: stmtId,
		}
	default:
		return &errD
	}
}

func (f *framer) writeHeader(flags byte, op frameOp, stream int) {
	f.wbuf = f.wbuf[:0]
	f.wbuf = append(f.wbuf,
		f.proto,
		flags,
	)

	if f.proto > protoVersion2 {
		f.wbuf = append(f.wbuf,
			byte(stream>>8),
			byte(stream),
		)
	} else {
		f.wbuf = append(f.wbuf,
			byte(stream),
		)
	}

	// pad out length
	f.wbuf = append(f.wbuf,
		byte(op),
		0,
		0,
		0,
		0,
	)
}

func (f *framer) setLength(length int) {
	p := 4
	if f.proto > protoVersion2 {
		p = 5
	}

	f.wbuf[p+0] = byte(length >> 24)
	f.wbuf[p+1] = byte(length >> 16)
	f.wbuf[p+2] = byte(length >> 8)
	f.wbuf[p+3] = byte(length)
}

func (f *framer) finishWrite() error {
	if f.wbuf[1]&flagCompress == flagCompress {
		if f.compres == nil {
			panic("compress flag set with no compressor")
		}

		// TODO: only compress frames which are big enough
		compressed, err := f.compres.Encode(f.wbuf[f.headSize:])
		if err != nil {
			return err
		}

		f.wbuf = append(f.wbuf[:f.headSize], compressed...)
	}
	length := len(f.wbuf) - f.headSize
	f.setLength(length)

	_, err := f.w.Write(f.wbuf)
	if err != nil {
		return err
	}

	return nil
}

func (f *framer) readTrace() {
	f.traceID = f.readUUID().Bytes()
}

type readyFrame struct {
	frameHeader
}

func (f *framer) parseReadyFrame() frame {
	return &readyFrame{
		frameHeader: *f.header,
	}
}

type supportedFrame struct {
	frameHeader

	supported map[string][]string
}

// TODO: if we move the body buffer onto the frameHeader then we only need a single
// framer, and can move the methods onto the header.
func (f *framer) parseSupportedFrame() frame {
	return &supportedFrame{
		frameHeader: *f.header,

		supported: f.readStringMultiMap(),
	}
}

type writeStartupFrame struct {
	opts map[string]string
}

func (w *writeStartupFrame) writeFrame(framer *framer, streamID int) error {
	return framer.writeStartupFrame(streamID, w.opts)
}

func (f *framer) writeStartupFrame(streamID int, options map[string]string) error {
	f.writeHeader(f.flags&^flagCompress, opStartup, streamID)
	f.writeStringMap(options)

	return f.finishWrite()
}

type writePrepareFrame struct {
	statement string
}

func (w *writePrepareFrame) writeFrame(framer *framer, streamID int) error {
	return framer.writePrepareFrame(streamID, w.statement)
}

func (f *framer) writePrepareFrame(stream int, statement string) error {
	f.writeHeader(f.flags, opPrepare, stream)
	f.writeLongString(statement)
	return f.finishWrite()
}

func (f *framer) readTypeInfo() *TypeInfo {
	id := f.readShort()
	typ := &TypeInfo{
		// we need to pass proto to the marshaller here
		Proto: f.proto,
		Type:  Type(id),
	}

	switch typ.Type {
	case TypeCustom:
		typ.Custom = f.readString()
		if cassType := getApacheCassandraType(typ.Custom); cassType != TypeCustom {
			typ = &TypeInfo{
				Proto: f.proto,
				Type:  cassType,
			}
			switch typ.Type {
			case TypeMap:
				typ.Key = f.readTypeInfo()
				fallthrough
			case TypeList, TypeSet:
				typ.Elem = f.readTypeInfo()
			}
		}
	case TypeMap:
		typ.Key = f.readTypeInfo()
		fallthrough
	case TypeList, TypeSet:
		typ.Elem = f.readTypeInfo()
	}

	return typ
}

type resultMetadata struct {
	flags int

	// only if flagPageState
	pagingState []byte

	columns []ColumnInfo
}

func (r resultMetadata) String() string {
	return fmt.Sprintf("[metadata flags=0x%x paging_state=% X columns=%v]", r.flags, r.pagingState, r.columns)
}

func (f *framer) parseResultMetadata() resultMetadata {
	meta := resultMetadata{
		flags: f.readInt(),
	}

	colCount := f.readInt()

	if meta.flags&flagHasMorePages == flagHasMorePages {
		meta.pagingState = f.readBytes()
	}

	if meta.flags&flagNoMetaData == flagNoMetaData {
		return meta
	}

	var keyspace, table string
	globalSpec := meta.flags&flagGlobalTableSpec == flagGlobalTableSpec
	if globalSpec {
		keyspace = f.readString()
		table = f.readString()
	}

	cols := make([]ColumnInfo, colCount)

	for i := 0; i < colCount; i++ {
		col := &cols[i]

		if !globalSpec {
			col.Keyspace = f.readString()
			col.Table = f.readString()
		} else {
			col.Keyspace = keyspace
			col.Table = table
		}

		col.Name = f.readString()
		col.TypeInfo = f.readTypeInfo()
	}

	meta.columns = cols

	return meta
}

type resultVoidFrame struct {
	frameHeader
}

func (f *resultVoidFrame) String() string {
	return "[result_void]"
}

func (f *framer) parseResultFrame() (frame, error) {
	kind := f.readInt()

	switch kind {
	case resultKindVoid:
		return &resultVoidFrame{frameHeader: *f.header}, nil
	case resultKindRows:
		return f.parseResultRows(), nil
	case resultKindKeyspace:
		return f.parseResultSetKeyspace(), nil
	case resultKindPrepared:
		return f.parseResultPrepared(), nil
	case resultKindSchemaChanged:
		return f.parseResultSchemaChange(), nil
	}

	return nil, NewErrProtocol("unknown result kind: %x", kind)
}

type resultRowsFrame struct {
	frameHeader

	meta resultMetadata
	rows [][][]byte
}

func (f *resultRowsFrame) String() string {
	return fmt.Sprintf("[result_rows meta=%v]", f.meta)
}

func (f *framer) parseResultRows() frame {
	meta := f.parseResultMetadata()

	numRows := f.readInt()
	colCount := len(meta.columns)

	rows := make([][][]byte, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = make([][]byte, colCount)
		for j := 0; j < colCount; j++ {
			rows[i][j] = f.readBytes()
		}
	}

	return &resultRowsFrame{
		frameHeader: *f.header,
		meta:        meta,
		rows:        rows,
	}
}

type resultKeyspaceFrame struct {
	frameHeader
	keyspace string
}

func (r *resultKeyspaceFrame) String() string {
	return fmt.Sprintf("[result_keyspace keyspace=%s]", r.keyspace)
}

func (f *framer) parseResultSetKeyspace() frame {
	return &resultKeyspaceFrame{
		frameHeader: *f.header,
		keyspace:    f.readString(),
	}
}

type resultPreparedFrame struct {
	frameHeader

	preparedID []byte
	reqMeta    resultMetadata
	respMeta   resultMetadata
}

func (f *framer) parseResultPrepared() frame {
	frame := &resultPreparedFrame{
		frameHeader: *f.header,
		preparedID:  f.readShortBytes(),
		reqMeta:     f.parseResultMetadata(),
	}

	if f.proto < protoVersion2 {
		return frame
	}

	frame.respMeta = f.parseResultMetadata()

	return frame
}

type resultSchemaChangeFrame struct {
	frameHeader

	change   string
	keyspace string
	table    string
}

func (s *resultSchemaChangeFrame) String() string {
	return fmt.Sprintf("[result_schema_change change=%s keyspace=%s table=%s]", s.change, s.keyspace, s.table)
}

func (f *framer) parseResultSchemaChange() frame {
	frame := &resultSchemaChangeFrame{
		frameHeader: *f.header,
	}

	if f.proto < protoVersion3 {
		frame.change = f.readString()
		frame.keyspace = f.readString()
		frame.table = f.readString()
	} else {
		// TODO: improve type representation of this
		frame.change = f.readString()
		target := f.readString()
		switch target {
		case "KEYSPACE":
			frame.keyspace = f.readString()
		case "TABLE", "TYPE":
			frame.keyspace = f.readString()
			frame.table = f.readString()
		}
	}

	return frame
}

type authenticateFrame struct {
	frameHeader

	class string
}

func (a *authenticateFrame) String() string {
	return fmt.Sprintf("[authenticate class=%q]", a.class)
}

func (f *framer) parseAuthenticateFrame() frame {
	return &authenticateFrame{
		frameHeader: *f.header,
		class:       f.readString(),
	}
}

type authSuccessFrame struct {
	frameHeader

	data []byte
}

func (a *authSuccessFrame) String() string {
	return fmt.Sprintf("[auth_success data=%q]", a.data)
}

func (f *framer) parseAuthSuccessFrame() frame {
	return &authSuccessFrame{
		frameHeader: *f.header,
		data:        f.readBytes(),
	}
}

type authChallengeFrame struct {
	frameHeader

	data []byte
}

func (a *authChallengeFrame) String() string {
	return fmt.Sprintf("[auth_challenge data=%q]", a.data)
}

func (f *framer) parseAuthChallengeFrame() frame {
	return &authChallengeFrame{
		frameHeader: *f.header,
		data:        f.readBytes(),
	}
}

type writeAuthResponseFrame struct {
	data []byte
}

func (a *writeAuthResponseFrame) String() string {
	return fmt.Sprintf("[auth_response data=%q]", a.data)
}

func (a *writeAuthResponseFrame) writeFrame(framer *framer, streamID int) error {
	return framer.writeAuthResponseFrame(streamID, a.data)
}

func (f *framer) writeAuthResponseFrame(streamID int, data []byte) error {
	f.writeHeader(f.flags, opAuthResponse, streamID)
	f.writeBytes(data)
	return f.finishWrite()
}

type queryValues struct {
	value []byte
	// optional name, will set With names for values flag
	name string
}

type queryParams struct {
	consistency Consistency
	// v2+
	skipMeta          bool
	values            []queryValues
	pageSize          int
	pagingState       []byte
	serialConsistency Consistency
	// v3+
	timestamp *time.Time
}

func (q queryParams) String() string {
	return fmt.Sprintf("[query_params consistency=%v skip_meta=%v page_size=%d paging_state=%q serial_consistency=%v timestamp=%v values=%v]",
		q.consistency, q.skipMeta, q.pageSize, q.pagingState, q.serialConsistency, q.timestamp, q.values)
}

func (f *framer) writeQueryParams(opts *queryParams) {
	f.writeConsistency(opts.consistency)

	if f.proto == protoVersion1 {
		return
	}

	var flags byte
	if len(opts.values) > 0 {
		flags |= flagValues
	}
	if opts.skipMeta {
		flags |= flagSkipMetaData
	}
	if opts.pageSize > 0 {
		flags |= flagPageSize
	}
	if len(opts.pagingState) > 0 {
		flags |= flagWithPagingState
	}
	if opts.serialConsistency > 0 {
		flags |= flagWithSerialConsistency
	}

	names := false

	// protoV3 specific things
	if f.proto > protoVersion2 {
		if opts.timestamp != nil {
			flags |= flagDefaultTimestamp
		}
		if len(opts.values) > 0 && opts.values[0].name != "" {
			flags |= flagWithNameValues
			names = true
		}
	}

	f.writeByte(flags)

	if n := len(opts.values); n > 0 {
		f.writeShort(uint16(n))
		for i := 0; i < n; i++ {
			if names {
				f.writeString(opts.values[i].name)
			}
			f.writeBytes(opts.values[i].value)
		}
	}

	if opts.pageSize > 0 {
		f.writeInt(int32(opts.pageSize))
	}

	if len(opts.pagingState) > 0 {
		f.writeBytes(opts.pagingState)
	}

	if opts.serialConsistency > 0 {
		f.writeConsistency(opts.serialConsistency)
	}

	if f.proto > protoVersion2 && opts.timestamp != nil {
		// timestamp in microseconds
		// TODO: should the timpestamp be set on the queryParams or should we set
		// it here?
		ts := opts.timestamp.UnixNano() / 1000
		f.writeLong(ts)
	}
}

type writeQueryFrame struct {
	statement string
	params    queryParams
}

func (w *writeQueryFrame) String() string {
	return fmt.Sprintf("[query statement=%q params=%v]", w.statement, w.params)
}

func (w *writeQueryFrame) writeFrame(framer *framer, streamID int) error {
	return framer.writeQueryFrame(streamID, w.statement, &w.params)
}

func (f *framer) writeQueryFrame(streamID int, statement string, params *queryParams) error {
	f.writeHeader(f.flags, opQuery, streamID)
	f.writeLongString(statement)
	f.writeQueryParams(params)

	return f.finishWrite()
}

type frameWriter interface {
	writeFrame(framer *framer, streamID int) error
}

type writeExecuteFrame struct {
	preparedID []byte
	params     queryParams
}

func (e *writeExecuteFrame) String() string {
	return fmt.Sprintf("[execute id=% X params=%v]", e.preparedID, &e.params)
}

func (e *writeExecuteFrame) writeFrame(fr *framer, streamID int) error {
	return fr.writeExecuteFrame(streamID, e.preparedID, &e.params)
}

func (f *framer) writeExecuteFrame(streamID int, preparedID []byte, params *queryParams) error {
	f.writeHeader(f.flags, opExecute, streamID)
	f.writeShortBytes(preparedID)
	if f.proto > protoVersion1 {
		f.writeQueryParams(params)
	} else {
		n := len(params.values)
		f.writeShort(uint16(n))
		for i := 0; i < n; i++ {
			f.writeBytes(params.values[i].value)
		}
		f.writeConsistency(params.consistency)
	}

	return f.finishWrite()
}

// TODO: can we replace BatchStatemt with batchStatement? As they prety much
// duplicate each other
type batchStatment struct {
	preparedID []byte
	statement  string
	values     []queryValues
}

type writeBatchFrame struct {
	typ               BatchType
	statements        []batchStatment
	consistency       Consistency
	serialConsistency Consistency
	defaultTimestamp  bool
}

func (w *writeBatchFrame) writeFrame(framer *framer, streamID int) error {
	return framer.writeBatchFrame(streamID, w)
}

func (f *framer) writeBatchFrame(streamID int, w *writeBatchFrame) error {
	f.writeHeader(f.flags, opBatch, streamID)
	f.writeByte(byte(w.typ))

	n := len(w.statements)
	f.writeShort(uint16(n))

	var flags byte

	for i := 0; i < n; i++ {
		b := &w.statements[i]
		if len(b.preparedID) == 0 {
			f.writeByte(0)
			f.writeLongString(b.statement)
			continue
		}

		f.writeByte(1)
		f.writeShortBytes(b.preparedID)
		f.writeShort(uint16(len(b.values)))
		for j := range b.values {
			col := &b.values[j]
			if f.proto > protoVersion2 && col.name != "" {
				// TODO: move this check into the caller and set a flag on writeBatchFrame
				// to indicate using named values
				flags |= flagWithNameValues
				f.writeString(col.name)
			}
			f.writeBytes(col.value)
		}
	}

	f.writeConsistency(w.consistency)

	if f.proto > protoVersion2 {
		if w.serialConsistency > 0 {
			flags |= flagWithSerialConsistency
		}
		if w.defaultTimestamp {
			flags |= flagDefaultTimestamp
		}

		f.writeByte(flags)

		if w.serialConsistency > 0 {
			f.writeConsistency(w.serialConsistency)
		}
		if w.defaultTimestamp {
			now := time.Now().UnixNano() / 1000
			f.writeLong(now)
		}
	}

	return f.finishWrite()
}

func (f *framer) readByte() byte {
	b := f.rbuf[0]
	f.rbuf = f.rbuf[1:]
	return b
}

func (f *framer) readInt() (n int) {
	n = int(int32(f.rbuf[0])<<24 | int32(f.rbuf[1])<<16 | int32(f.rbuf[2])<<8 | int32(f.rbuf[3]))
	f.rbuf = f.rbuf[4:]
	return
}

func (f *framer) readShort() (n uint16) {
	n = uint16(f.rbuf[0])<<8 | uint16(f.rbuf[1])
	f.rbuf = f.rbuf[2:]
	return
}

func (f *framer) readLong() (n int64) {
	n = int64(f.rbuf[0])<<56 | int64(f.rbuf[1])<<48 | int64(f.rbuf[2])<<40 | int64(f.rbuf[3])<<32 |
		int64(f.rbuf[4])<<24 | int64(f.rbuf[5])<<16 | int64(f.rbuf[6])<<8 | int64(f.rbuf[7])
	f.rbuf = f.rbuf[8:]
	return
}

func (f *framer) readString() (s string) {
	size := f.readShort()
	s = string(f.rbuf[:size])
	f.rbuf = f.rbuf[size:]
	return
}

func (f *framer) readLongString() (s string) {
	size := f.readInt()
	s = string(f.rbuf[:size])
	f.rbuf = f.rbuf[size:]
	return
}

func (f *framer) readUUID() *UUID {
	// TODO: how to handle this error, if it is a uuid, then sureley, problems?
	u, _ := UUIDFromBytes(f.rbuf[:16])
	f.rbuf = f.rbuf[16:]
	return &u
}

func (f *framer) readStringList() []string {
	size := f.readShort()

	l := make([]string, size)
	for i := 0; i < int(size); i++ {
		l[i] = f.readString()
	}

	return l
}

func (f *framer) readBytes() []byte {
	size := f.readInt()
	if size < 0 {
		return nil
	}

	// we cant make assumptions about the length of the life of the supplied byte
	// slice so we defensivly copy it out of the underlying buffer. This has the
	// downside of increasing allocs per read but will provide much greater memory
	// safety. The allocs can hopefully be improved in the future.
	// TODO: dont copy into a new slice
	l := make([]byte, size)
	copy(l, f.rbuf[:size])
	f.rbuf = f.rbuf[size:]

	return l
}

func (f *framer) readShortBytes() []byte {
	size := f.readShort()

	l := make([]byte, size)
	copy(l, f.rbuf[:size])
	f.rbuf = f.rbuf[size:]

	return l
}

func (f *framer) readInet() (net.IP, int) {
	size := f.rbuf[0]
	f.rbuf = f.rbuf[1:]

	if !(size == 4 || size == 16) {
		panic(fmt.Sprintf("invalid IP size: %d", size))
	}

	ip := make([]byte, size)
	copy(ip, f.rbuf[:size])
	f.rbuf = f.rbuf[size:]

	port := f.readInt()
	return net.IP(ip), port
}

func (f *framer) readConsistency() Consistency {
	return Consistency(f.readShort())
}

func (f *framer) readStringMap() map[string]string {
	size := f.readShort()
	m := make(map[string]string)

	for i := 0; i < int(size); i++ {
		k := f.readString()
		v := f.readString()
		m[k] = v
	}

	return m
}

func (f *framer) readStringMultiMap() map[string][]string {
	size := f.readShort()
	m := make(map[string][]string)

	for i := 0; i < int(size); i++ {
		k := f.readString()
		v := f.readStringList()
		m[k] = v
	}

	return m
}

func (f *framer) writeByte(b byte) {
	f.wbuf = append(f.wbuf, b)
}

// these are protocol level binary types
func (f *framer) writeInt(n int32) {
	f.wbuf = append(f.wbuf,
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n),
	)
}

func (f *framer) writeShort(n uint16) {
	f.wbuf = append(f.wbuf,
		byte(n>>8),
		byte(n),
	)
}

func (f *framer) writeLong(n int64) {
	f.wbuf = append(f.wbuf,
		byte(n>>56),
		byte(n>>48),
		byte(n>>40),
		byte(n>>32),
		byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n),
	)
}

func (f *framer) writeString(s string) {
	f.writeShort(uint16(len(s)))
	f.wbuf = append(f.wbuf, s...)
}

func (f *framer) writeLongString(s string) {
	f.writeInt(int32(len(s)))
	f.wbuf = append(f.wbuf, s...)
}

func (f *framer) writeUUID(u *UUID) {
	f.wbuf = append(f.wbuf, u[:]...)
}

func (f *framer) writeStringList(l []string) {
	f.writeShort(uint16(len(l)))
	for _, s := range l {
		f.writeString(s)
	}
}

func (f *framer) writeBytes(p []byte) {
	// TODO: handle null case correctly,
	//     [bytes]        A [int] n, followed by n bytes if n >= 0. If n < 0,
	//					  no byte should follow and the value represented is `null`.
	if p == nil {
		f.writeInt(-1)
	} else {
		f.writeInt(int32(len(p)))
		f.wbuf = append(f.wbuf, p...)
	}
}

func (f *framer) writeShortBytes(p []byte) {
	f.writeShort(uint16(len(p)))
	f.wbuf = append(f.wbuf, p...)
}

func (f *framer) writeInet(ip net.IP, port int) {
	f.wbuf = append(f.wbuf,
		byte(len(ip)),
	)

	f.wbuf = append(f.wbuf,
		[]byte(ip)...,
	)

	f.writeInt(int32(port))
}

func (f *framer) writeConsistency(cons Consistency) {
	f.writeShort(uint16(cons))
}

func (f *framer) writeStringMap(m map[string]string) {
	f.writeShort(uint16(len(m)))
	for k, v := range m {
		f.writeString(k)
		f.writeString(v)
	}
}
