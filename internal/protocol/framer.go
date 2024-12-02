package protocol

import (
	"errors"
	"fmt"
	"github.com/gocql/gocql/internal/compressor"
	"github.com/gocql/gocql/internal/internal_errors"
	"io"
	"io/ioutil"
	"net"
	"runtime"
	"strings"
	"time"
)

type UnsetColumn struct{}

var (
	ErrFrameTooBig = errors.New("frame length is bigger than the maximum allowed")
)

// UnsetValue represents a value used in a query binding that will be ignored by Cassandra.
//
// By setting a field to the unset value Cassandra will ignore the write completely.
// The main advantage is the ability to keep the same prepared statement even when you don't
// want to update some fields, where before you needed to make another prepared statement.
//
// UnsetValue is only available when using the version 4 of the protocol.
//var UnsetValue = UnsetColumn{}

type NamedValue struct {
	Name  string
	Value interface{}
}

const (
	ProtoDirectionMask = 0x80
	ProtoVersionMask   = 0x7F
	ProtoVersion1      = 0x01
	ProtoVersion2      = 0x02
	ProtoVersion3      = 0x03
	ProtoVersion4      = 0x04
	ProtoVersion5      = 0x05

	MaxFrameSize = 256 * 1024 * 1024
)

type ProtoVersion byte

func (p ProtoVersion) request() bool {
	return p&ProtoDirectionMask == 0x00
}

func (p ProtoVersion) response() bool {
	return p&ProtoDirectionMask == 0x80
}

func (p ProtoVersion) Version() byte {
	return byte(p) & ProtoVersionMask
}

func (p ProtoVersion) String() string {
	dir := "REQ"
	if p.response() {
		dir = "RESP"
	}

	return fmt.Sprintf("[version=%d direction=%s]", p.Version(), dir)
}

type FrameOp byte

const (
	// header ops
	opError         FrameOp = 0x00
	opStartup       FrameOp = 0x01
	opReady         FrameOp = 0x02
	opAuthenticate  FrameOp = 0x03
	opOptions       FrameOp = 0x05
	opSupported     FrameOp = 0x06
	opQuery         FrameOp = 0x07
	opResult        FrameOp = 0x08
	opPrepare       FrameOp = 0x09
	opExecute       FrameOp = 0x0A
	opRegister      FrameOp = 0x0B
	opEvent         FrameOp = 0x0C
	opBatch         FrameOp = 0x0D
	opAuthChallenge FrameOp = 0x0E
	opAuthResponse  FrameOp = 0x0F
	opAuthSuccess   FrameOp = 0x10
)

func (f FrameOp) String() string {
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
	flagHasMorePages    int = 0x02
	flagNoMetaData      int = 0x04

	// query flags
	flagValues                byte = 0x01
	flagSkipMetaData          byte = 0x02
	flagPageSize              byte = 0x04
	flagWithPagingState       byte = 0x08
	flagWithSerialConsistency byte = 0x10
	flagDefaultTimestamp      byte = 0x20
	flagWithNameValues        byte = 0x40
	flagWithKeyspace          byte = 0x80

	// prepare flags
	flagWithPreparedKeyspace uint32 = 0x01

	// header flags
	flagCompress      byte = 0x01
	flagTracing       byte = 0x02
	flagCustomPayload byte = 0x04
	flagWarning       byte = 0x08
	flagBetaProtocol  byte = 0x10
)

type Consistency uint16

const (
	Any         Consistency = 0x00
	One         Consistency = 0x01
	Two         Consistency = 0x02
	Three       Consistency = 0x03
	Quorum      Consistency = 0x04
	All         Consistency = 0x05
	LocalQuorum Consistency = 0x06
	EachQuorum  Consistency = 0x07
	LocalOne    Consistency = 0x0A
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
	case LocalOne:
		return "LOCAL_ONE"
	default:
		return fmt.Sprintf("UNKNOWN_CONS_0x%x", uint16(c))
	}
}

func (c Consistency) MarshalText() (text []byte, err error) {
	return []byte(c.String()), nil
}

func (c *Consistency) UnmarshalText(text []byte) error {
	switch string(text) {
	case "ANY":
		*c = Any
	case "ONE":
		*c = One
	case "TWO":
		*c = Two
	case "THREE":
		*c = Three
	case "QUORUM":
		*c = Quorum
	case "ALL":
		*c = All
	case "LOCAL_QUORUM":
		*c = LocalQuorum
	case "EACH_QUORUM":
		*c = EachQuorum
	case "LOCAL_ONE":
		*c = LocalOne
	default:
		return fmt.Errorf("invalid consistency %q", string(text))
	}

	return nil
}

func ParseConsistency(s string) Consistency {
	var c Consistency
	if err := c.UnmarshalText([]byte(strings.ToUpper(s))); err != nil {
		panic(err)
	}
	return c
}

// ParseConsistencyWrapper wraps gocql.ParseConsistency to provide an err
// return instead of a panic
func ParseConsistencyWrapper(s string) (consistency Consistency, err error) {
	err = consistency.UnmarshalText([]byte(strings.ToUpper(s)))
	return
}

// MustParseConsistency is the same as ParseConsistency except it returns
// an error (never). It is kept here since breaking changes are not good.
// DEPRECATED: use ParseConsistency if you want a panic on parse error.
func MustParseConsistency(s string) (Consistency, error) {
	c, err := ParseConsistencyWrapper(s)
	if err != nil {
		panic(err)
	}
	return c, nil
}

type SerialConsistency uint16

const (
	Serial      SerialConsistency = 0x08
	LocalSerial SerialConsistency = 0x09
)

func (s SerialConsistency) String() string {
	switch s {
	case Serial:
		return "SERIAL"
	case LocalSerial:
		return "LOCAL_SERIAL"
	default:
		return fmt.Sprintf("UNKNOWN_SERIAL_CONS_0x%x", uint16(s))
	}
}

func (s SerialConsistency) MarshalText() (text []byte, err error) {
	return []byte(s.String()), nil
}

func (s *SerialConsistency) UnmarshalText(text []byte) error {
	switch string(text) {
	case "SERIAL":
		*s = Serial
	case "LOCAL_SERIAL":
		*s = LocalSerial
	default:
		return fmt.Errorf("invalid consistency %q", string(text))
	}

	return nil
}

const (
	ApacheCassandraTypePrefix = "org.apache.cassandra.db.marshal."
)

const MaxFrameHeaderSize = 9

func ReadInt(p []byte) int32 {
	return int32(p[0])<<24 | int32(p[1])<<16 | int32(p[2])<<8 | int32(p[3])
}

type FrameHeader struct {
	Version  ProtoVersion
	Flags    byte
	Stream   int
	Op       FrameOp
	Length   int
	Warnings []string
}

func (f FrameHeader) String() string {
	return fmt.Sprintf("[header version=%s flags=0x%x stream=%d op=%s length=%d]", f.Version, f.Flags, f.Stream, f.Op, f.Length)
}

func (f FrameHeader) Header() FrameHeader {
	return f
}

const defaultBufSize = 128

// a Framer is responsible for reading, writing and parsing frames on a single stream
type Framer struct {
	proto byte
	// flags are for outgoing flags, enabling compression and tracing etc
	flags    byte
	compres  compressor.Compressor
	headSize int
	// if this frame was read then the header will be here
	Header *FrameHeader

	// if tracing flag is set this is not nil
	TraceID []byte

	// holds a ref to the whole byte slice for buf so that it can be reset to
	// 0 after a read.
	readBuffer []byte

	Buf []byte

	CustomPayload map[string][]byte
}

func NewFramer(compressor compressor.Compressor, version byte) *Framer {
	buf := make([]byte, defaultBufSize)
	f := &Framer{
		Buf:        buf[:0],
		readBuffer: buf,
	}
	var flags byte
	if compressor != nil {
		flags |= flagCompress
	}
	if version == ProtoVersion5 {
		flags |= flagBetaProtocol
	}

	version &= ProtoVersionMask

	headSize := 8
	if version > ProtoVersion2 {
		headSize = 9
	}

	f.compres = compressor
	f.proto = version
	f.flags = flags
	f.headSize = headSize

	f.Header = nil
	f.TraceID = nil

	return f
}

type Frame interface {
	Header() FrameHeader
}

func ReadHeader(r io.Reader, p []byte) (head FrameHeader, err error) {
	_, err = io.ReadFull(r, p[:1])
	if err != nil {
		return FrameHeader{}, err
	}

	version := p[0] & ProtoVersionMask

	if version < ProtoVersion1 || version > ProtoVersion5 {
		return FrameHeader{}, fmt.Errorf("gocql: unsupported protocol response version: %d", version)
	}

	headSize := 9
	if version < ProtoVersion3 {
		headSize = 8
	}

	_, err = io.ReadFull(r, p[1:headSize])
	if err != nil {
		return FrameHeader{}, err
	}

	p = p[:headSize]

	head.Version = ProtoVersion(p[0])
	head.Flags = p[1]

	if version > ProtoVersion2 {
		if len(p) != 9 {
			return FrameHeader{}, fmt.Errorf("not enough bytes to read header require 9 got: %d", len(p))
		}

		head.Stream = int(int16(p[2])<<8 | int16(p[3]))
		head.Op = FrameOp(p[4])
		head.Length = int(ReadInt(p[5:]))
	} else {
		if len(p) != 8 {
			return FrameHeader{}, fmt.Errorf("not enough bytes to read header require 8 got: %d", len(p))
		}

		head.Stream = int(int8(p[2]))
		head.Op = FrameOp(p[3])
		head.Length = int(ReadInt(p[4:]))
	}

	return head, nil
}

// explicitly enables tracing for the Framers outgoing requests
func (f *Framer) Trace() {
	f.flags |= flagTracing
}

// explicitly enables the custom payload flag
func (f *Framer) payload() {
	f.flags |= flagCustomPayload
}

// reads a frame form the wire into the Framers buffer
func (f *Framer) ReadFrame(r io.Reader, head *FrameHeader) error {
	if head.Length < 0 {
		return fmt.Errorf("frame body length can not be less than 0: %d", head.Length)
	} else if head.Length > MaxFrameSize {
		// need to free up the connection to be used again
		_, err := io.CopyN(ioutil.Discard, r, int64(head.Length))
		if err != nil {
			return fmt.Errorf("error whilst trying to discard frame with invalid length: %v", err)
		}
		return ErrFrameTooBig
	}

	if cap(f.readBuffer) >= head.Length {
		f.Buf = f.readBuffer[:head.Length]
	} else {
		f.readBuffer = make([]byte, head.Length)
		f.Buf = f.readBuffer
	}

	// assume the underlying reader takes care of timeouts and retries
	n, err := io.ReadFull(r, f.Buf)
	if err != nil {
		return fmt.Errorf("unable to read frame body: read %d/%d bytes: %v", n, head.Length, err)
	}

	if head.Flags&flagCompress == flagCompress {
		if f.compres == nil {
			return NewErrProtocol("no compressor available with compressed frame body")
		}

		f.Buf, err = f.compres.Decode(f.Buf)
		if err != nil {
			return err
		}
	}

	f.Header = head
	return nil
}

func (f *Framer) ParseFrame() (frame Frame, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
		}
	}()

	if f.Header.Version.request() {
		return nil, NewErrProtocol("got a request frame from server: %v", f.Header.Version)
	}

	if f.Header.Flags&flagTracing == flagTracing {
		f.readTrace()
	}

	if f.Header.Flags&flagWarning == flagWarning {
		f.Header.Warnings = f.readStringList()
	}

	if f.Header.Flags&flagCustomPayload == flagCustomPayload {
		f.CustomPayload = f.readBytesMap()
	}

	// assumes that the frame body has been read into rbuf
	switch f.Header.Op {
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
	case opEvent:
		frame = f.parseEventFrame()
	default:
		return nil, NewErrProtocol("unknown op in frame header: %s", f.Header.Op)
	}

	return
}

func (f *Framer) parseErrorFrame() Frame {
	code := f.readInt()
	msg := f.readString()

	errD := internal_errors.ErrorFrame{
		FrameHeader: *f.Header,
		Cod:         code,
		Messag:      msg,
	}

	switch code {
	case internal_errors.ErrCodeUnavailable:
		cl := f.readConsistency()
		required := f.readInt()
		alive := f.readInt()
		return &internal_errors.RequestErrUnavailable{
			ErrorFrame:  errD,
			Consistency: cl,
			Required:    required,
			Alive:       alive,
		}
	case internal_errors.ErrCodeWriteTimeout:
		cl := f.readConsistency()
		received := f.readInt()
		blockfor := f.readInt()
		writeType := f.readString()
		return &internal_errors.RequestErrWriteTimeout{
			ErrorFrame:  errD,
			Consistency: cl,
			Received:    received,
			BlockFor:    blockfor,
			WriteType:   writeType,
		}
	case internal_errors.ErrCodeReadTimeout:
		cl := f.readConsistency()
		received := f.readInt()
		blockfor := f.readInt()
		dataPresent := f.readByte()
		return &internal_errors.RequestErrReadTimeout{
			ErrorFrame:  errD,
			Consistency: cl,
			Received:    received,
			BlockFor:    blockfor,
			DataPresent: dataPresent,
		}
	case internal_errors.ErrCodeAlreadyExists:
		ks := f.readString()
		table := f.readString()
		return &internal_errors.RequestErrAlreadyExists{
			ErrorFrame: errD,
			Keyspace:   ks,
			Table:      table,
		}
	case internal_errors.ErrCodeUnprepared:
		stmtId := f.readShortBytes()
		return &internal_errors.RequestErrUnprepared{
			ErrorFrame:  errD,
			StatementId: CopyBytes(stmtId), // defensively copy
		}
	case internal_errors.ErrCodeReadFailure:
		res := &internal_errors.RequestErrReadFailure{
			ErrorFrame: errD,
		}
		res.Consistency = f.readConsistency()
		res.Received = f.readInt()
		res.BlockFor = f.readInt()
		if f.proto > ProtoVersion4 {
			res.ErrorMap = f.readErrorMap()
			res.NumFailures = len(res.ErrorMap)
		} else {
			res.NumFailures = f.readInt()
		}
		res.DataPresent = f.readByte() != 0

		return res
	case internal_errors.ErrCodeWriteFailure:
		res := &internal_errors.RequestErrWriteFailure{
			ErrorFrame: errD,
		}
		res.Consistency = f.readConsistency()
		res.Received = f.readInt()
		res.BlockFor = f.readInt()
		if f.proto > ProtoVersion4 {
			res.ErrorMap = f.readErrorMap()
			res.NumFailures = len(res.ErrorMap)
		} else {
			res.NumFailures = f.readInt()
		}
		res.WriteType = f.readString()
		return res
	case internal_errors.ErrCodeFunctionFailure:
		res := &internal_errors.RequestErrFunctionFailure{
			ErrorFrame: errD,
		}
		res.Keyspace = f.readString()
		res.Function = f.readString()
		res.ArgTypes = f.readStringList()
		return res

	case internal_errors.ErrCodeCDCWriteFailure:
		res := &internal_errors.RequestErrCDCWriteFailure{
			ErrorFrame: errD,
		}
		return res
	case internal_errors.ErrCodeCASWriteUnknown:
		res := &internal_errors.RequestErrCASWriteUnknown{
			ErrorFrame: errD,
		}
		res.Consistency = f.readConsistency()
		res.Received = f.readInt()
		res.BlockFor = f.readInt()
		return res
	case internal_errors.ErrCodeInvalid, internal_errors.ErrCodeBootstrapping, internal_errors.ErrCodeConfig, internal_errors.ErrCodeCredentials, internal_errors.ErrCodeOverloaded,
		internal_errors.ErrCodeProtocol, internal_errors.ErrCodeServer, internal_errors.ErrCodeSyntax, internal_errors.ErrCodeTruncate, internal_errors.ErrCodeUnauthorized:
		// TODO(zariel): we should have some distinct types for these internal_errors
		return errD
	default:
		panic(fmt.Errorf("unknown error code: 0x%x", errD.Cod))
	}
}

func (f *Framer) readErrorMap() (errMap internal_errors.ErrorMap) {
	errMap = make(internal_errors.ErrorMap)
	numErrs := f.readInt()
	for i := 0; i < numErrs; i++ {
		ip := f.readInetAdressOnly().String()
		errMap[ip] = f.readShort()
	}
	return
}

func (f *Framer) writeHeader(flags byte, op FrameOp, stream int) {
	f.Buf = f.Buf[:0]
	f.Buf = append(f.Buf,
		f.proto,
		flags,
	)

	if f.proto > ProtoVersion2 {
		f.Buf = append(f.Buf,
			byte(stream>>8),
			byte(stream),
		)
	} else {
		f.Buf = append(f.Buf,
			byte(stream),
		)
	}

	// pad out length
	f.Buf = append(f.Buf,
		byte(op),
		0,
		0,
		0,
		0,
	)
}

func (f *Framer) setLength(length int) {
	p := 4
	if f.proto > ProtoVersion2 {
		p = 5
	}

	f.Buf[p+0] = byte(length >> 24)
	f.Buf[p+1] = byte(length >> 16)
	f.Buf[p+2] = byte(length >> 8)
	f.Buf[p+3] = byte(length)
}

func (f *Framer) finish() error {
	if len(f.Buf) > MaxFrameSize {
		// huge app frame, lets remove it so it doesn't bloat the heap
		f.Buf = make([]byte, defaultBufSize)
		return ErrFrameTooBig
	}

	if f.Buf[1]&flagCompress == flagCompress {
		if f.compres == nil {
			panic("compress flag set with no compressor")
		}

		// TODO: only compress frames which are big enough
		compressed, err := f.compres.Encode(f.Buf[f.headSize:])
		if err != nil {
			return err
		}

		f.Buf = append(f.Buf[:f.headSize], compressed...)
	}
	length := len(f.Buf) - f.headSize
	f.setLength(length)

	return nil
}

func (f *Framer) writeTo(w io.Writer) error {
	_, err := w.Write(f.Buf)
	return err
}

func (f *Framer) readTrace() {
	f.TraceID = f.readUUID().Bytes()
}

type ReadyFrame struct {
	FrameHeader
}

func (f *Framer) parseReadyFrame() Frame {
	return &ReadyFrame{
		FrameHeader: *f.Header,
	}
}

type SupportedFrame struct {
	FrameHeader

	Supported map[string][]string
}

// TODO: if we move the body buffer onto the FrameHeader then we only need a single
// Framer, and can move the methods onto the header.
func (f *Framer) parseSupportedFrame() Frame {
	return &SupportedFrame{
		FrameHeader: *f.Header,

		Supported: f.readStringMultiMap(),
	}
}

type WriteStartupFrame struct {
	Opts map[string]string
}

func (w WriteStartupFrame) String() string {
	return fmt.Sprintf("[startup opts=%+v]", w.Opts)
}

func (w *WriteStartupFrame) BuildFrame(f *Framer, streamID int) error {
	f.writeHeader(f.flags&^flagCompress, opStartup, streamID)
	f.writeStringMap(w.Opts)

	return f.finish()
}

type WritePrepareFrame struct {
	Statement     string
	Keyspace      string
	customPayload map[string][]byte
}

func (w *WritePrepareFrame) BuildFrame(f *Framer, streamID int) error {
	if len(w.customPayload) > 0 {
		f.payload()
	}
	f.writeHeader(f.flags, opPrepare, streamID)
	f.writeCustomPayload(&w.customPayload)
	f.writeLongString(w.Statement)

	var flags uint32 = 0
	if w.Keyspace != "" {
		if f.proto > ProtoVersion4 {
			flags |= flagWithPreparedKeyspace
		} else {
			panic(fmt.Errorf("the keyspace can only be set with protocol 5 or higher"))
		}
	}
	if f.proto > ProtoVersion4 {
		f.writeUint(flags)
	}
	if w.Keyspace != "" {
		f.writeString(w.Keyspace)
	}

	return f.finish()
}

func (f *Framer) readTypeInfo() TypeInfo {
	// TODO: factor this out so the same code paths can be used to parse custom
	// types and other types, as much of the logic will be duplicated.
	id := f.readShort()

	simple := NativeType{
		proto: f.proto,
		Typ:   Type(id),
	}

	if simple.Typ == TypeCustom {
		simple.Cust = f.readString()
		if cassType := GetApacheCassandraType(simple.Cust); cassType != TypeCustom {
			simple.Typ = cassType
		}
	}

	switch simple.Typ {
	case TypeTuple:
		n := f.readShort()
		tuple := TupleTypeInfo{
			NativeType: simple,
			Elems:      make([]TypeInfo, n),
		}

		for i := 0; i < int(n); i++ {
			tuple.Elems[i] = f.readTypeInfo()
		}

		return tuple

	case TypeUDT:
		udt := UDTTypeInfo{
			NativeType: simple,
		}
		udt.KeySpace = f.readString()
		udt.Name = f.readString()

		n := f.readShort()
		udt.Elements = make([]UDTField, n)
		for i := 0; i < int(n); i++ {
			field := &udt.Elements[i]
			field.Name = f.readString()
			field.Type = f.readTypeInfo()
		}

		return udt
	case TypeMap, TypeList, TypeSet:
		collection := CollectionType{
			NativeType: simple,
		}

		if simple.Typ == TypeMap {
			collection.Key = f.readTypeInfo()
		}

		collection.Elem = f.readTypeInfo()

		return collection
	}

	return simple
}

type PreparedMetadata struct {
	ResultMetadata

	// proto v4+
	PkeyColumns []int

	Keyspace string

	Table string
}

func (r PreparedMetadata) String() string {
	return fmt.Sprintf("[prepared flags=0x%x pkey=%v paging_state=% X columns=%v col_count=%d actual_col_count=%d]", r.Flags, r.PkeyColumns, r.PagingState, r.Columns, r.ColCount, r.ActualColCount)
}

func (f *Framer) parsePreparedMetadata() PreparedMetadata {
	// TODO: deduplicate this from parseMetadata
	meta := PreparedMetadata{}

	meta.Flags = f.readInt()
	meta.ColCount = f.readInt()
	if meta.ColCount < 0 {
		panic(fmt.Errorf("received negative column count: %d", meta.ColCount))
	}
	meta.ActualColCount = meta.ColCount

	if f.proto >= ProtoVersion4 {
		pkeyCount := f.readInt()
		pkeys := make([]int, pkeyCount)
		for i := 0; i < pkeyCount; i++ {
			pkeys[i] = int(f.readShort())
		}
		meta.PkeyColumns = pkeys
	}

	if meta.Flags&flagHasMorePages == flagHasMorePages {
		meta.PagingState = CopyBytes(f.readBytes())
	}

	if meta.Flags&flagNoMetaData == flagNoMetaData {
		return meta
	}

	globalSpec := meta.Flags&flagGlobalTableSpec == flagGlobalTableSpec
	if globalSpec {
		meta.Keyspace = f.readString()
		meta.Table = f.readString()
	}

	var cols []ColumnInfo
	if meta.ColCount < 1000 {
		// preallocate columninfo to avoid excess copying
		cols = make([]ColumnInfo, meta.ColCount)
		for i := 0; i < meta.ColCount; i++ {
			f.readCol(&cols[i], &meta.ResultMetadata, globalSpec, meta.Keyspace, meta.Table)
		}
	} else {
		// use append, huge number of columns usually indicates a corrupt frame or
		// just a huge row.
		for i := 0; i < meta.ColCount; i++ {
			var col ColumnInfo
			f.readCol(&col, &meta.ResultMetadata, globalSpec, meta.Keyspace, meta.Table)
			cols = append(cols, col)
		}
	}

	meta.Columns = cols

	return meta
}

type ResultMetadata struct {
	Flags int

	// only if flagPageState
	PagingState []byte

	Columns  []ColumnInfo
	ColCount int

	// this is a count of the total number of columns which can be scanned,
	// it is at minimum len(columns) but may be larger, for instance when a column
	// is a UDT or tuple.
	ActualColCount int
}

func (r *ResultMetadata) MorePages() bool {
	return r.Flags&flagHasMorePages == flagHasMorePages
}

func (r ResultMetadata) String() string {
	return fmt.Sprintf("[metadata flags=0x%x paging_state=% X columns=%v]", r.Flags, r.PagingState, r.Columns)
}

func (f *Framer) readCol(col *ColumnInfo, meta *ResultMetadata, globalSpec bool, keyspace, table string) {
	if !globalSpec {
		col.Keyspace = f.readString()
		col.Table = f.readString()
	} else {
		col.Keyspace = keyspace
		col.Table = table
	}

	col.Name = f.readString()
	col.TypeInfo = f.readTypeInfo()
	switch v := col.TypeInfo.(type) {
	// maybe also UDT
	case TupleTypeInfo:
		// -1 because we already included the tuple column
		meta.ActualColCount += len(v.Elems) - 1
	}
}

func (f *Framer) parseResultMetadata() ResultMetadata {
	var meta ResultMetadata

	meta.Flags = f.readInt()
	meta.ColCount = f.readInt()
	if meta.ColCount < 0 {
		panic(fmt.Errorf("received negative column count: %d", meta.ColCount))
	}
	meta.ActualColCount = meta.ColCount

	if meta.Flags&flagHasMorePages == flagHasMorePages {
		meta.PagingState = CopyBytes(f.readBytes())
	}

	if meta.Flags&flagNoMetaData == flagNoMetaData {
		return meta
	}

	var keyspace, table string
	globalSpec := meta.Flags&flagGlobalTableSpec == flagGlobalTableSpec
	if globalSpec {
		keyspace = f.readString()
		table = f.readString()
	}

	var cols []ColumnInfo
	if meta.ColCount < 1000 {
		// preallocate columninfo to avoid excess copying
		cols = make([]ColumnInfo, meta.ColCount)
		for i := 0; i < meta.ColCount; i++ {
			f.readCol(&cols[i], &meta, globalSpec, keyspace, table)
		}

	} else {
		// use append, huge number of columns usually indicates a corrupt frame or
		// just a huge row.
		for i := 0; i < meta.ColCount; i++ {
			var col ColumnInfo
			f.readCol(&col, &meta, globalSpec, keyspace, table)
			cols = append(cols, col)
		}
	}

	meta.Columns = cols

	return meta
}

type ResultVoidFrame struct {
	FrameHeader
}

func (f *ResultVoidFrame) String() string {
	return "[result_void]"
}

func (f *Framer) parseResultFrame() (Frame, error) {
	kind := f.readInt()

	switch kind {
	case resultKindVoid:
		return &ResultVoidFrame{FrameHeader: *f.Header}, nil
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

type ResultRowsFrame struct {
	FrameHeader

	Meta ResultMetadata
	// dont parse the rows here as we only need to do it once
	NumRows int
}

func (f *ResultRowsFrame) String() string {
	return fmt.Sprintf("[result_rows meta=%v]", f.Meta)
}

func (f *Framer) parseResultRows() Frame {
	result := &ResultRowsFrame{}
	result.Meta = f.parseResultMetadata()

	result.NumRows = f.readInt()
	if result.NumRows < 0 {
		panic(fmt.Errorf("invalid row_count in result frame: %d", result.NumRows))
	}

	return result
}

type ResultKeyspaceFrame struct {
	FrameHeader
	keyspace string
}

func (r *ResultKeyspaceFrame) String() string {
	return fmt.Sprintf("[result_keyspace keyspace=%s]", r.keyspace)
}

func (f *Framer) parseResultSetKeyspace() Frame {
	return &ResultKeyspaceFrame{
		FrameHeader: *f.Header,
		keyspace:    f.readString(),
	}
}

type ResultPreparedFrame struct {
	FrameHeader

	PreparedID []byte
	ReqMeta    PreparedMetadata
	RespMeta   ResultMetadata
}

func (f *Framer) parseResultPrepared() Frame {
	frame := &ResultPreparedFrame{
		FrameHeader: *f.Header,
		PreparedID:  f.readShortBytes(),
		ReqMeta:     f.parsePreparedMetadata(),
	}

	if f.proto < ProtoVersion2 {
		return frame
	}

	frame.RespMeta = f.parseResultMetadata()

	return frame
}

type SchemaChangeKeyspace struct {
	FrameHeader

	Change   string
	Keyspace string
}

func (f SchemaChangeKeyspace) String() string {
	return fmt.Sprintf("[event schema_change_keyspace change=%q keyspace=%q]", f.Change, f.Keyspace)
}

type SchemaChangeTable struct {
	FrameHeader

	change   string
	Keyspace string
	object   string
}

func (f SchemaChangeTable) String() string {
	return fmt.Sprintf("[event schema_change change=%q keyspace=%q object=%q]", f.change, f.Keyspace, f.object)
}

type SchemaChangeType struct {
	FrameHeader

	change   string
	Keyspace string
	object   string
}

type SchemaChangeFunction struct {
	FrameHeader

	change   string
	Keyspace string
	name     string
	args     []string
}

type SchemaChangeAggregate struct {
	FrameHeader

	change   string
	Keyspace string
	name     string
	args     []string
}

func (f *Framer) parseResultSchemaChange() Frame {
	if f.proto <= ProtoVersion2 {
		change := f.readString()
		keyspace := f.readString()
		table := f.readString()

		if table != "" {
			return &SchemaChangeTable{
				FrameHeader: *f.Header,
				change:      change,
				Keyspace:    keyspace,
				object:      table,
			}
		} else {
			return &SchemaChangeKeyspace{
				FrameHeader: *f.Header,
				Change:      change,
				Keyspace:    keyspace,
			}
		}
	} else {
		change := f.readString()
		target := f.readString()

		// TODO: could just use a separate type for each target
		switch target {
		case "KEYSPACE":
			frame := &SchemaChangeKeyspace{
				FrameHeader: *f.Header,
				Change:      change,
			}

			frame.Keyspace = f.readString()

			return frame
		case "TABLE":
			frame := &SchemaChangeTable{
				FrameHeader: *f.Header,
				change:      change,
			}

			frame.Keyspace = f.readString()
			frame.object = f.readString()

			return frame
		case "TYPE":
			frame := &SchemaChangeType{
				FrameHeader: *f.Header,
				change:      change,
			}

			frame.Keyspace = f.readString()
			frame.object = f.readString()

			return frame
		case "FUNCTION":
			frame := &SchemaChangeFunction{
				FrameHeader: *f.Header,
				change:      change,
			}

			frame.Keyspace = f.readString()
			frame.name = f.readString()
			frame.args = f.readStringList()

			return frame
		case "AGGREGATE":
			frame := &SchemaChangeAggregate{
				FrameHeader: *f.Header,
				change:      change,
			}

			frame.Keyspace = f.readString()
			frame.name = f.readString()
			frame.args = f.readStringList()

			return frame
		default:
			panic(fmt.Errorf("gocql: unknown SCHEMA_CHANGE target: %q change: %q", target, change))
		}
	}

}

type AuthenticateFrame struct {
	FrameHeader

	Class string
}

func (a *AuthenticateFrame) String() string {
	return fmt.Sprintf("[authenticate class=%q]", a.Class)
}

func (f *Framer) parseAuthenticateFrame() Frame {
	return &AuthenticateFrame{
		FrameHeader: *f.Header,
		Class:       f.readString(),
	}
}

type AuthSuccessFrame struct {
	FrameHeader

	Data []byte
}

func (a *AuthSuccessFrame) String() string {
	return fmt.Sprintf("[auth_success data=%q]", a.Data)
}

func (f *Framer) parseAuthSuccessFrame() Frame {
	return &AuthSuccessFrame{
		FrameHeader: *f.Header,
		Data:        f.readBytes(),
	}
}

type AuthChallengeFrame struct {
	FrameHeader

	Data []byte
}

func (a *AuthChallengeFrame) String() string {
	return fmt.Sprintf("[auth_challenge data=%q]", a.Data)
}

func (f *Framer) parseAuthChallengeFrame() Frame {
	return &AuthChallengeFrame{
		FrameHeader: *f.Header,
		Data:        f.readBytes(),
	}
}

type StatusChangeEventFrame struct {
	FrameHeader

	Change string
	Host   net.IP
	Port   int
}

func (t StatusChangeEventFrame) String() string {
	return fmt.Sprintf("[status_change change=%s host=%v port=%v]", t.Change, t.Host, t.Port)
}

// essentially the same as statusChange
type TopologyChangeEventFrame struct {
	FrameHeader

	change string
	host   net.IP
	port   int
}

func (t TopologyChangeEventFrame) String() string {
	return fmt.Sprintf("[topology_change change=%s host=%v port=%v]", t.change, t.host, t.port)
}

func (f *Framer) parseEventFrame() Frame {
	eventType := f.readString()

	switch eventType {
	case "TOPOLOGY_CHANGE":
		frame := &TopologyChangeEventFrame{FrameHeader: *f.Header}
		frame.change = f.readString()
		frame.host, frame.port = f.readInet()

		return frame
	case "STATUS_CHANGE":
		frame := &StatusChangeEventFrame{FrameHeader: *f.Header}
		frame.Change = f.readString()
		frame.Host, frame.Port = f.readInet()

		return frame
	case "SCHEMA_CHANGE":
		// this should work for all versions
		return f.parseResultSchemaChange()
	default:
		panic(fmt.Errorf("gocql: unknown event type: %q", eventType))
	}

}

type WriteAuthResponseFrame struct {
	Data []byte
}

func (a *WriteAuthResponseFrame) String() string {
	return fmt.Sprintf("[auth_response data=%q]", a.Data)
}

func (a *WriteAuthResponseFrame) BuildFrame(framer *Framer, streamID int) error {
	return framer.WriteAuthResponseFrame(streamID, a.Data)
}

func (f *Framer) WriteAuthResponseFrame(streamID int, data []byte) error {
	f.writeHeader(f.flags, opAuthResponse, streamID)
	f.writeBytes(data)
	return f.finish()
}

type QueryValues struct {
	Value []byte

	// optional name, will set With names for values flag
	Name    string
	IsUnset bool
}

type QueryParams struct {
	Consistency
	// v2+
	SkipMeta          bool
	Values            []QueryValues
	PageSize          int
	PagingState       []byte
	SerialConsistency SerialConsistency
	// v3+
	DefaultTimestamp      bool
	DefaultTimestampValue int64
	// v5+
	Keyspace string
}

func (q QueryParams) String() string {
	return fmt.Sprintf("[query_params consistency=%v skip_meta=%v page_size=%d paging_state=%q serial_consistency=%v default_timestamp=%v values=%v keyspace=%s]",
		q.Consistency, q.SkipMeta, q.PageSize, q.PagingState, q.SerialConsistency, q.DefaultTimestamp, q.Values, q.Keyspace)
}

func (f *Framer) writeQueryParams(opts *QueryParams) {
	f.writeConsistency(opts.Consistency)

	if f.proto == ProtoVersion1 {
		return
	}

	var flags byte
	if len(opts.Values) > 0 {
		flags |= flagValues
	}
	if opts.SkipMeta {
		flags |= flagSkipMetaData
	}
	if opts.PageSize > 0 {
		flags |= flagPageSize
	}
	if len(opts.PagingState) > 0 {
		flags |= flagWithPagingState
	}
	if opts.SerialConsistency > 0 {
		flags |= flagWithSerialConsistency
	}

	names := false

	// protoV3 specific things
	if f.proto > ProtoVersion2 {
		if opts.DefaultTimestamp {
			flags |= flagDefaultTimestamp
		}

		if len(opts.Values) > 0 && opts.Values[0].Name != "" {
			flags |= flagWithNameValues
			names = true
		}
	}

	if opts.Keyspace != "" {
		if f.proto > ProtoVersion4 {
			flags |= flagWithKeyspace
		} else {
			panic(fmt.Errorf("the keyspace can only be set with protocol 5 or higher"))
		}
	}

	if f.proto > ProtoVersion4 {
		f.writeUint(uint32(flags))
	} else {
		f.writeByte(flags)
	}

	if n := len(opts.Values); n > 0 {
		f.writeShort(uint16(n))

		for i := 0; i < n; i++ {
			if names {
				f.writeString(opts.Values[i].Name)
			}
			if opts.Values[i].IsUnset {
				f.writeUnset()
			} else {
				f.writeBytes(opts.Values[i].Value)
			}
		}
	}

	if opts.PageSize > 0 {
		f.writeInt(int32(opts.PageSize))
	}

	if len(opts.PagingState) > 0 {
		f.writeBytes(opts.PagingState)
	}

	if opts.SerialConsistency > 0 {
		f.writeConsistency(Consistency(opts.SerialConsistency))
	}

	if f.proto > ProtoVersion2 && opts.DefaultTimestamp {
		// timestamp in microseconds
		var ts int64
		if opts.DefaultTimestampValue != 0 {
			ts = opts.DefaultTimestampValue
		} else {
			ts = time.Now().UnixNano() / 1000
		}
		f.writeLong(ts)
	}

	if opts.Keyspace != "" {
		f.writeString(opts.Keyspace)
	}
}

type WriteQueryFrame struct {
	Statement string
	Params    QueryParams

	// v4+
	CustomPayload map[string][]byte
}

func (w *WriteQueryFrame) String() string {
	return fmt.Sprintf("[query statement=%q params=%v]", w.Statement, w.Params)
}

func (w *WriteQueryFrame) BuildFrame(framer *Framer, streamID int) error {
	return framer.WriteQueryFrame(streamID, w.Statement, &w.Params, w.CustomPayload)
}

func (f *Framer) WriteQueryFrame(streamID int, statement string, params *QueryParams, customPayload map[string][]byte) error {
	if len(customPayload) > 0 {
		f.payload()
	}
	f.writeHeader(f.flags, opQuery, streamID)
	f.writeCustomPayload(&customPayload)
	f.writeLongString(statement)
	f.writeQueryParams(params)

	return f.finish()
}

type FrameBuilder interface {
	BuildFrame(framer *Framer, streamID int) error
}

type frameWriterFunc func(framer *Framer, streamID int) error

func (f frameWriterFunc) BuildFrame(framer *Framer, streamID int) error {
	return f(framer, streamID)
}

type WriteExecuteFrame struct {
	PreparedID []byte
	Params     QueryParams

	// v4+
	CustomPayload map[string][]byte
}

func (e *WriteExecuteFrame) String() string {
	return fmt.Sprintf("[execute id=% X params=%v]", e.PreparedID, &e.Params)
}

func (e *WriteExecuteFrame) BuildFrame(fr *Framer, streamID int) error {
	return fr.WriteExecuteFrame(streamID, e.PreparedID, &e.Params, &e.CustomPayload)
}

func (f *Framer) WriteExecuteFrame(streamID int, preparedID []byte, params *QueryParams, customPayload *map[string][]byte) error {
	if len(*customPayload) > 0 {
		f.payload()
	}
	f.writeHeader(f.flags, opExecute, streamID)
	f.writeCustomPayload(customPayload)
	f.writeShortBytes(preparedID)
	if f.proto > ProtoVersion1 {
		f.writeQueryParams(params)
	} else {
		n := len(params.Values)
		f.writeShort(uint16(n))
		for i := 0; i < n; i++ {
			if params.Values[i].IsUnset {
				f.writeUnset()
			} else {
				f.writeBytes(params.Values[i].Value)
			}
		}
		f.writeConsistency(params.Consistency)
	}

	return f.finish()
}

// TODO: can we replace BatchStatemt with batchStatement? As they prety much
// duplicate each other
type BatchStatment struct {
	PreparedID []byte
	Statement  string
	Values     []QueryValues
}

type WriteBatchFrame struct {
	Typ         BatchType
	Statements  []BatchStatment
	Consistency Consistency

	// v3+
	SerialConsistency     SerialConsistency
	DefaultTimestamp      bool
	DefaultTimestampValue int64

	//v4+
	CustomPayload map[string][]byte
}

func (w *WriteBatchFrame) BuildFrame(framer *Framer, streamID int) error {
	return framer.WriteBatchFrame(streamID, w, w.CustomPayload)
}

func (f *Framer) WriteBatchFrame(streamID int, w *WriteBatchFrame, customPayload map[string][]byte) error {
	if len(customPayload) > 0 {
		f.payload()
	}
	f.writeHeader(f.flags, opBatch, streamID)
	f.writeCustomPayload(&customPayload)
	f.writeByte(byte(w.Typ))

	n := len(w.Statements)
	f.writeShort(uint16(n))

	var flags byte

	for i := 0; i < n; i++ {
		b := &w.Statements[i]
		if len(b.PreparedID) == 0 {
			f.writeByte(0)
			f.writeLongString(b.Statement)
		} else {
			f.writeByte(1)
			f.writeShortBytes(b.PreparedID)
		}

		f.writeShort(uint16(len(b.Values)))
		for j := range b.Values {
			col := b.Values[j]
			if f.proto > ProtoVersion2 && col.Name != "" {
				// TODO: move this check into the caller and set a flag on WriteBatchFrame
				// to indicate using named values
				if f.proto <= ProtoVersion5 {
					return fmt.Errorf("gocql: named query values are not supported in batches, please see https://issues.apache.org/jira/browse/CASSANDRA-10246")
				}
				flags |= flagWithNameValues
				f.writeString(col.Name)
			}
			if col.IsUnset {
				f.writeUnset()
			} else {
				f.writeBytes(col.Value)
			}
		}
	}

	f.writeConsistency(w.Consistency)

	if f.proto > ProtoVersion2 {
		if w.SerialConsistency > 0 {
			flags |= flagWithSerialConsistency
		}
		if w.DefaultTimestamp {
			flags |= flagDefaultTimestamp
		}

		if f.proto > ProtoVersion4 {
			f.writeUint(uint32(flags))
		} else {
			f.writeByte(flags)
		}

		if w.SerialConsistency > 0 {
			f.writeConsistency(Consistency(w.SerialConsistency))
		}

		if w.DefaultTimestamp {
			var ts int64
			if w.DefaultTimestampValue != 0 {
				ts = w.DefaultTimestampValue
			} else {
				ts = time.Now().UnixNano() / 1000
			}
			f.writeLong(ts)
		}
	}

	return f.finish()
}

type WriteOptionsFrame struct{}

func (w *WriteOptionsFrame) BuildFrame(framer *Framer, streamID int) error {
	return framer.WriteOptionsFrame(streamID, w)
}

func (f *Framer) WriteOptionsFrame(stream int, _ *WriteOptionsFrame) error {
	f.writeHeader(f.flags&^flagCompress, opOptions, stream)
	return f.finish()
}

type WriteRegisterFrame struct {
	Events []string
}

func (w *WriteRegisterFrame) BuildFrame(framer *Framer, streamID int) error {
	return framer.WriteRegisterFrame(streamID, w)
}

func (f *Framer) WriteRegisterFrame(streamID int, w *WriteRegisterFrame) error {
	f.writeHeader(f.flags, opRegister, streamID)
	f.writeStringList(w.Events)

	return f.finish()
}

func (f *Framer) readByte() byte {
	if len(f.Buf) < 1 {
		panic(fmt.Errorf("not enough bytes in buffer to read byte require 1 got: %d", len(f.Buf)))
	}

	b := f.Buf[0]
	f.Buf = f.Buf[1:]
	return b
}

func (f *Framer) readInt() (n int) {
	if len(f.Buf) < 4 {
		panic(fmt.Errorf("not enough bytes in buffer to read int require 4 got: %d", len(f.Buf)))
	}

	n = int(int32(f.Buf[0])<<24 | int32(f.Buf[1])<<16 | int32(f.Buf[2])<<8 | int32(f.Buf[3]))
	f.Buf = f.Buf[4:]
	return
}

func (f *Framer) readShort() (n uint16) {
	if len(f.Buf) < 2 {
		panic(fmt.Errorf("not enough bytes in buffer to read short require 2 got: %d", len(f.Buf)))
	}
	n = uint16(f.Buf[0])<<8 | uint16(f.Buf[1])
	f.Buf = f.Buf[2:]
	return
}

func (f *Framer) readString() (s string) {
	size := f.readShort()

	if len(f.Buf) < int(size) {
		panic(fmt.Errorf("not enough bytes in buffer to read string require %d got: %d", size, len(f.Buf)))
	}

	s = string(f.Buf[:size])
	f.Buf = f.Buf[size:]
	return
}

func (f *Framer) readLongString() (s string) {
	size := f.readInt()

	if len(f.Buf) < size {
		panic(fmt.Errorf("not enough bytes in buffer to read long string require %d got: %d", size, len(f.Buf)))
	}

	s = string(f.Buf[:size])
	f.Buf = f.Buf[size:]
	return
}

func (f *Framer) readUUID() *UUID {
	if len(f.Buf) < 16 {
		panic(fmt.Errorf("not enough bytes in buffer to read uuid require %d got: %d", 16, len(f.Buf)))
	}

	// TODO: how to handle this error, if it is a uuid, then sureley, problems?
	u, _ := UUIDFromBytes(f.Buf[:16])
	f.Buf = f.Buf[16:]
	return &u
}

func (f *Framer) readStringList() []string {
	size := f.readShort()

	l := make([]string, size)
	for i := 0; i < int(size); i++ {
		l[i] = f.readString()
	}

	return l
}

func (f *Framer) ReadBytesInternal() ([]byte, error) {
	size := f.readInt()
	if size < 0 {
		return nil, nil
	}

	if len(f.Buf) < size {
		return nil, fmt.Errorf("not enough bytes in buffer to read bytes require %d got: %d", size, len(f.Buf))
	}

	l := f.Buf[:size]
	f.Buf = f.Buf[size:]

	return l, nil
}

func (f *Framer) readBytes() []byte {
	l, err := f.ReadBytesInternal()
	if err != nil {
		panic(err)
	}

	return l
}

func (f *Framer) readShortBytes() []byte {
	size := f.readShort()
	if len(f.Buf) < int(size) {
		panic(fmt.Errorf("not enough bytes in buffer to read short bytes: require %d got %d", size, len(f.Buf)))
	}

	l := f.Buf[:size]
	f.Buf = f.Buf[size:]

	return l
}

func (f *Framer) readInetAdressOnly() net.IP {
	if len(f.Buf) < 1 {
		panic(fmt.Errorf("not enough bytes in buffer to read inet size require %d got: %d", 1, len(f.Buf)))
	}

	size := f.Buf[0]
	f.Buf = f.Buf[1:]

	if !(size == 4 || size == 16) {
		panic(fmt.Errorf("invalid IP size: %d", size))
	}

	if len(f.Buf) < 1 {
		panic(fmt.Errorf("not enough bytes in buffer to read inet require %d got: %d", size, len(f.Buf)))
	}

	ip := make([]byte, size)
	copy(ip, f.Buf[:size])
	f.Buf = f.Buf[size:]
	return net.IP(ip)
}

func (f *Framer) readInet() (net.IP, int) {
	return f.readInetAdressOnly(), f.readInt()
}

func (f *Framer) readConsistency() Consistency {
	return Consistency(f.readShort())
}

func (f *Framer) readBytesMap() map[string][]byte {
	size := f.readShort()
	m := make(map[string][]byte, size)

	for i := 0; i < int(size); i++ {
		k := f.readString()
		v := f.readBytes()
		m[k] = v
	}

	return m
}

func (f *Framer) readStringMultiMap() map[string][]string {
	size := f.readShort()
	m := make(map[string][]string, size)

	for i := 0; i < int(size); i++ {
		k := f.readString()
		v := f.readStringList()
		m[k] = v
	}

	return m
}

func (f *Framer) writeByte(b byte) {
	f.Buf = append(f.Buf, b)
}

func appendBytes(p []byte, d []byte) []byte {
	if d == nil {
		return appendInt(p, -1)
	}
	p = appendInt(p, int32(len(d)))
	p = append(p, d...)
	return p
}

func appendShort(p []byte, n uint16) []byte {
	return append(p,
		byte(n>>8),
		byte(n),
	)
}

func appendInt(p []byte, n int32) []byte {
	return append(p, byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n))
}

func appendUint(p []byte, n uint32) []byte {
	return append(p, byte(n>>24),
		byte(n>>16),
		byte(n>>8),
		byte(n))
}

func appendLong(p []byte, n int64) []byte {
	return append(p,
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

func (f *Framer) writeCustomPayload(customPayload *map[string][]byte) {
	if len(*customPayload) > 0 {
		if f.proto < ProtoVersion4 {
			panic("Custom payload is not supported with version V3 or less")
		}
		f.writeBytesMap(*customPayload)
	}
}

// these are protocol level binary types
func (f *Framer) writeInt(n int32) {
	f.Buf = appendInt(f.Buf, n)
}

func (f *Framer) writeUint(n uint32) {
	f.Buf = appendUint(f.Buf, n)
}

func (f *Framer) writeShort(n uint16) {
	f.Buf = appendShort(f.Buf, n)
}

func (f *Framer) writeLong(n int64) {
	f.Buf = appendLong(f.Buf, n)
}

func (f *Framer) writeString(s string) {
	f.writeShort(uint16(len(s)))
	f.Buf = append(f.Buf, s...)
}

func (f *Framer) writeLongString(s string) {
	f.writeInt(int32(len(s)))
	f.Buf = append(f.Buf, s...)
}

func (f *Framer) writeStringList(l []string) {
	f.writeShort(uint16(len(l)))
	for _, s := range l {
		f.writeString(s)
	}
}

func (f *Framer) writeUnset() {
	// Protocol version 4 specifies that bind variables do not require having a
	// value when executing a statement.   Bind variables without a value are
	// called 'unset'. The 'unset' bind variable is serialized as the int
	// value '-2' without following bytes.
	f.writeInt(-2)
}

func (f *Framer) writeBytes(p []byte) {
	// TODO: handle null case correctly,
	//     [bytes]        A [int] n, followed by n bytes if n >= 0. If n < 0,
	//					  no byte should follow and the value represented is `null`.
	if p == nil {
		f.writeInt(-1)
	} else {
		f.writeInt(int32(len(p)))
		f.Buf = append(f.Buf, p...)
	}
}

func (f *Framer) writeShortBytes(p []byte) {
	f.writeShort(uint16(len(p)))
	f.Buf = append(f.Buf, p...)
}

func (f *Framer) writeConsistency(cons Consistency) {
	f.writeShort(uint16(cons))
}

func (f *Framer) writeStringMap(m map[string]string) {
	f.writeShort(uint16(len(m)))
	for k, v := range m {
		f.writeString(k)
		f.writeString(v)
	}
}

func (f *Framer) writeBytesMap(m map[string][]byte) {
	f.writeShort(uint16(len(m)))
	for k, v := range m {
		f.writeString(k)
		f.writeBytes(v)
	}
}

type ErrProtocol struct{ error }

func NewErrProtocol(format string, args ...interface{}) error {
	return ErrProtocol{fmt.Errorf(format, args...)}
}
