// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
	"fmt"
	"net"
)

const (
	protoDirectionMask = 0x80
	protoVersionMask   = 0x7F
	protoVersion1      = 0x01
	protoVersion2      = 0x02
	protoVersion3      = 0x03

	opError         byte = 0x00
	opStartup       byte = 0x01
	opReady         byte = 0x02
	opAuthenticate  byte = 0x03
	opOptions       byte = 0x05
	opSupported     byte = 0x06
	opQuery         byte = 0x07
	opResult        byte = 0x08
	opPrepare       byte = 0x09
	opExecute       byte = 0x0A
	opRegister      byte = 0x0B
	opEvent         byte = 0x0C
	opBatch         byte = 0x0D
	opAuthChallenge byte = 0x0E
	opAuthResponse  byte = 0x0F
	opAuthSuccess   byte = 0x10

	resultKindVoid          = 1
	resultKindRows          = 2
	resultKindKeyspace      = 3
	resultKindPrepared      = 4
	resultKindSchemaChanged = 5

	flagQueryValues uint8 = 1
	flagCompress    uint8 = 1
	flagTrace       uint8 = 2
	flagPageSize    uint8 = 4
	flagPageState   uint8 = 8
	flagHasMore     uint8 = 2

	apacheCassandraTypePrefix = "org.apache.cassandra.db.marshal."
)

var headerProtoSize = [...]int{
	protoVersion1: 8,
	protoVersion2: 8,
	protoVersion3: 9,
}

// TODO: replace with a struct which has a header and a body buffer,
// header just has methods like, set/get the options in its backing array
// then in a writeTo we write the header then the body.
type frame []byte

func newFrame(version uint8) frame {
	// TODO: pool these at the session level incase anyone is using different
	// clusters with different versions in the same application.
	return make(frame, headerProtoSize[version], defaultFrameSize)
}

func (f *frame) writeInt(v int32) {
	p := f.grow(4)
	(*f)[p] = byte(v >> 24)
	(*f)[p+1] = byte(v >> 16)
	(*f)[p+2] = byte(v >> 8)
	(*f)[p+3] = byte(v)
}

func (f *frame) writeShort(v uint16) {
	p := f.grow(2)
	(*f)[p] = byte(v >> 8)
	(*f)[p+1] = byte(v)
}

func (f *frame) writeString(v string) {
	f.writeShort(uint16(len(v)))
	p := f.grow(len(v))
	copy((*f)[p:], v)
}

func (f *frame) writeLongString(v string) {
	f.writeInt(int32(len(v)))
	p := f.grow(len(v))
	copy((*f)[p:], v)
}

func (f *frame) writeUUID() {
}

func (f *frame) writeStringList(v []string) {
	f.writeShort(uint16(len(v)))
	for i := range v {
		f.writeString(v[i])
	}
}

func (f *frame) writeByte(v byte) {
	p := f.grow(1)
	(*f)[p] = v
}

func (f *frame) writeBytes(v []byte) {
	if v == nil {
		f.writeInt(-1)
		return
	}
	f.writeInt(int32(len(v)))
	p := f.grow(len(v))
	copy((*f)[p:], v)
}

func (f *frame) writeShortBytes(v []byte) {
	f.writeShort(uint16(len(v)))
	p := f.grow(len(v))
	copy((*f)[p:], v)
}

func (f *frame) writeInet(ip net.IP, port int) {
	p := f.grow(1 + len(ip))
	(*f)[p] = byte(len(ip))
	copy((*f)[p+1:], ip)
	f.writeInt(int32(port))
}

func (f *frame) writeStringMap(v map[string]string) {
	f.writeShort(uint16(len(v)))
	for key, value := range v {
		f.writeString(key)
		f.writeString(value)
	}
}

func (f *frame) writeStringMultimap(v map[string][]string) {
	f.writeShort(uint16(len(v)))
	for key, values := range v {
		f.writeString(key)
		f.writeStringList(values)
	}
}

func (f *frame) setHeader(version, flags uint8, stream int, opcode uint8) {
	(*f)[0] = version
	(*f)[1] = flags
	p := 2
	if version&maskVersion > protoVersion2 {
		(*f)[2] = byte(stream >> 8)
		(*f)[3] = byte(stream)
		p += 2
	} else {
		(*f)[2] = byte(stream & 0xFF)
		p++
	}

	(*f)[p] = opcode
}

func (f *frame) setStream(stream int, version uint8) {
	if version > protoVersion2 {
		(*f)[2] = byte(stream >> 8)
		(*f)[3] = byte(stream)
	} else {
		(*f)[2] = byte(stream)
	}
}

func (f *frame) Stream(version uint8) (n int) {
	if version > protoVersion2 {
		n = int((*f)[2])<<8 | int((*f)[3])
	} else {
		n = int((*f)[2])
	}
	return
}

func (f *frame) setLength(length int, version uint8) {
	p := 4
	if version > protoVersion2 {
		p = 5
	}

	(*f)[p] = byte(length >> 24)
	(*f)[p+1] = byte(length >> 16)
	(*f)[p+2] = byte(length >> 8)
	(*f)[p+3] = byte(length)
}

func (f *frame) Op(version uint8) byte {
	if version > protoVersion2 {
		return (*f)[4]
	} else {
		return (*f)[3]
	}
}

func (f *frame) Length(version uint8) int {
	p := 4
	if version > protoVersion2 {
		p = 5
	}

	return int((*f)[p])<<24 | int((*f)[p+1])<<16 | int((*f)[p+2])<<8 | int((*f)[p+3])
}

func (f *frame) grow(n int) int {
	if len(*f)+n >= cap(*f) {
		buf := make(frame, len(*f), len(*f)*2+n)
		copy(buf, *f)
		*f = buf
	}
	p := len(*f)
	*f = (*f)[:p+n]
	return p
}

func (f *frame) skipHeader(version uint8) {
	*f = (*f)[headerProtoSize[version]:]
}

func (f *frame) readInt() int {
	if len(*f) < 4 {
		panic(NewErrProtocol("Trying to read an int while <4 bytes in the buffer"))
	}
	v := uint32((*f)[0])<<24 | uint32((*f)[1])<<16 | uint32((*f)[2])<<8 | uint32((*f)[3])
	*f = (*f)[4:]
	return int(int32(v))
}

func (f *frame) readShort() uint16 {
	if len(*f) < 2 {
		panic(NewErrProtocol("Trying to read a short while <2 bytes in the buffer"))
	}
	v := uint16((*f)[0])<<8 | uint16((*f)[1])
	*f = (*f)[2:]
	return v
}

func (f *frame) readString() string {
	n := int(f.readShort())
	if len(*f) < n {
		panic(NewErrProtocol("Trying to read a string of %d bytes from a buffer with %d bytes in it", n, len(*f)))
	}
	v := string((*f)[:n])
	*f = (*f)[n:]
	return v
}

func (f *frame) readLongString() string {
	n := f.readInt()
	if len(*f) < n {
		panic(NewErrProtocol("Trying to read a string of %d bytes from a buffer with %d bytes in it", n, len(*f)))
	}
	v := string((*f)[:n])
	*f = (*f)[n:]
	return v
}

func (f *frame) readBytes() []byte {
	n := f.readInt()
	if n < 0 {
		return nil
	}
	if len(*f) < n {
		panic(NewErrProtocol("Trying to read %d bytes from a buffer with %d bytes in it", n, len(*f)))
	}
	v := (*f)[:n]
	*f = (*f)[n:]
	return v
}

func (f *frame) readShortBytes() []byte {
	n := int(f.readShort())
	if len(*f) < n {
		panic(NewErrProtocol("Trying to read %d bytes from a buffer with %d bytes in it", n, len(*f)))
	}
	v := (*f)[:n]
	*f = (*f)[n:]
	return v
}

func (f *frame) readTypeInfo(version uint8) *TypeInfo {
	x := f.readShort()
	typ := &TypeInfo{
		Proto: version,
		Type:  Type(x),
	}
	switch typ.Type {
	case TypeCustom:
		typ.Custom = f.readString()
		if cassType := getApacheCassandraType(typ.Custom); cassType != TypeCustom {
			typ = &TypeInfo{Type: cassType}
			switch typ.Type {
			case TypeMap:
				typ.Key = f.readTypeInfo(version)
				fallthrough
			case TypeList, TypeSet:
				typ.Elem = f.readTypeInfo(version)
			}
		}
	case TypeMap:
		typ.Key = f.readTypeInfo(version)
		fallthrough
	case TypeList, TypeSet:
		typ.Elem = f.readTypeInfo(version)
	}
	return typ
}

func (f *frame) readMetaData(version uint8) ([]ColumnInfo, []byte) {
	flags := f.readInt()
	numColumns := f.readInt()

	var pageState []byte
	if flags&2 != 0 {
		pageState = f.readBytes()
	}

	globalKeyspace := ""
	globalTable := ""
	if flags&1 != 0 {
		globalKeyspace = f.readString()
		globalTable = f.readString()
	}

	columns := make([]ColumnInfo, numColumns)
	for i := 0; i < numColumns; i++ {
		columns[i].Keyspace = globalKeyspace
		columns[i].Table = globalTable
		if flags&1 == 0 {
			columns[i].Keyspace = f.readString()
			columns[i].Table = f.readString()
		}
		columns[i].Name = f.readString()
		columns[i].TypeInfo = f.readTypeInfo(version)
	}
	return columns, pageState
}

func (f *frame) readError() RequestError {
	code := f.readInt()
	msg := f.readString()
	errD := errorFrame{code, msg}
	switch code {
	case errUnavailable:
		cl := Consistency(f.readShort())
		required := f.readInt()
		alive := f.readInt()
		return RequestErrUnavailable{errorFrame: errD,
			Consistency: cl,
			Required:    required,
			Alive:       alive}
	case errWriteTimeout:
		cl := Consistency(f.readShort())
		received := f.readInt()
		blockfor := f.readInt()
		writeType := f.readString()
		return RequestErrWriteTimeout{errorFrame: errD,
			Consistency: cl,
			Received:    received,
			BlockFor:    blockfor,
			WriteType:   writeType,
		}
	case errReadTimeout:
		cl := Consistency(f.readShort())
		received := f.readInt()
		blockfor := f.readInt()
		dataPresent := (*f)[0]
		*f = (*f)[1:]
		return RequestErrReadTimeout{errorFrame: errD,
			Consistency: cl,
			Received:    received,
			BlockFor:    blockfor,
			DataPresent: dataPresent,
		}
	case errAlreadyExists:
		ks := f.readString()
		table := f.readString()
		return RequestErrAlreadyExists{errorFrame: errD,
			Keyspace: ks,
			Table:    table,
		}
	case errUnprepared:
		stmtId := f.readShortBytes()
		return RequestErrUnprepared{errorFrame: errD,
			StatementId: stmtId,
		}
	default:
		return errD
	}
}

func (f *frame) writeConsistency(c Consistency) {
	f.writeShort(consistencyCodes[c])
}

func (f frame) encodeFrame(version uint8, dst frame) (frame, error) {
	return f, nil
}

var consistencyCodes = []uint16{
	Any:         0x0000,
	One:         0x0001,
	Two:         0x0002,
	Three:       0x0003,
	Quorum:      0x0004,
	All:         0x0005,
	LocalQuorum: 0x0006,
	EachQuorum:  0x0007,
	Serial:      0x0008,
	LocalSerial: 0x0009,
	LocalOne:    0x000A,
}

type readyFrame struct{}

type supportedFrame struct{}

type resultVoidFrame struct{}

type resultRowsFrame struct {
	Columns     []ColumnInfo
	Rows        [][][]byte
	PagingState []byte
}

type resultKeyspaceFrame struct {
	Keyspace string
}

type resultPreparedFrame struct {
	PreparedId   []byte
	Arguments    []ColumnInfo
	ReturnValues []ColumnInfo
}

type operation interface {
	encodeFrame(version uint8, dst frame) (frame, error)
}

type startupFrame struct {
	CQLVersion  string
	Compression string
}

func (op *startupFrame) String() string {
	return fmt.Sprintf("[startup cqlversion=%q compression=%q]", op.CQLVersion, op.Compression)
}

func (op *startupFrame) encodeFrame(version uint8, f frame) (frame, error) {
	if f == nil {
		f = newFrame(version)
	}

	f.setHeader(version, 0, 0, opStartup)

	// TODO: fix this, this is actually a StringMap
	var size uint16 = 1
	if op.Compression != "" {
		size++
	}

	f.writeShort(size)
	f.writeString("CQL_VERSION")
	f.writeString(op.CQLVersion)

	if op.Compression != "" {
		f.writeString("COMPRESSION")
		f.writeString(op.Compression)
	}

	return f, nil
}

type queryFrame struct {
	Stmt      string
	Prepared  []byte
	Cons      Consistency
	Values    [][]byte
	PageSize  int
	PageState []byte
}

func (op *queryFrame) String() string {
	return fmt.Sprintf("[query statement=%q prepared=%x cons=%v ...]", op.Stmt, op.Prepared, op.Cons)
}

func (op *queryFrame) encodeFrame(version uint8, f frame) (frame, error) {
	if version == 1 && (op.PageSize != 0 || len(op.PageState) > 0 ||
		(len(op.Values) > 0 && len(op.Prepared) == 0)) {
		return nil, ErrUnsupported
	}

	if f == nil {
		f = newFrame(version)
	}

	if len(op.Prepared) > 0 {
		f.setHeader(version, 0, 0, opExecute)
		f.writeShortBytes(op.Prepared)
	} else {
		f.setHeader(version, 0, 0, opQuery)
		f.writeLongString(op.Stmt)
	}

	if version >= 2 {
		f.writeConsistency(op.Cons)
		flagPos := len(f)
		f.writeByte(0)

		if len(op.Values) > 0 {
			f[flagPos] |= flagQueryValues
			f.writeShort(uint16(len(op.Values)))
			for _, value := range op.Values {
				f.writeBytes(value)
			}
		}

		if op.PageSize > 0 {
			f[flagPos] |= flagPageSize
			f.writeInt(int32(op.PageSize))
		}

		if len(op.PageState) > 0 {
			f[flagPos] |= flagPageState
			f.writeBytes(op.PageState)
		}
	} else if version == 1 {
		if len(op.Prepared) > 0 {
			f.writeShort(uint16(len(op.Values)))
			for _, value := range op.Values {
				f.writeBytes(value)
			}
		}
		f.writeConsistency(op.Cons)
	}

	return f, nil
}

type prepareFrame struct {
	Stmt string
}

func (op *prepareFrame) String() string {
	return fmt.Sprintf("[prepare statement=%q]", op.Stmt)
}

func (op *prepareFrame) encodeFrame(version uint8, f frame) (frame, error) {
	if f == nil {
		f = newFrame(version)
	}
	f.setHeader(version, 0, 0, opPrepare)
	f.writeLongString(op.Stmt)
	return f, nil
}

type optionsFrame struct{}

func (op *optionsFrame) String() string {
	return "[options]"
}

func (op *optionsFrame) encodeFrame(version uint8, f frame) (frame, error) {
	if f == nil {
		f = newFrame(version)
	}
	f.setHeader(version, 0, 0, opOptions)
	return f, nil
}

type authenticateFrame struct {
	Authenticator string
}

func (op *authenticateFrame) String() string {
	return fmt.Sprintf("[authenticate authenticator=%q]", op.Authenticator)
}

type authResponseFrame struct {
	Data []byte
}

func (op *authResponseFrame) String() string {
	return fmt.Sprintf("[auth_response data=%q]", op.Data)
}

func (op *authResponseFrame) encodeFrame(version uint8, f frame) (frame, error) {
	if f == nil {
		f = newFrame(version)
	}
	f.setHeader(version, 0, 0, opAuthResponse)
	f.writeBytes(op.Data)
	return f, nil
}

type authSuccessFrame struct {
	Data []byte
}

func (op *authSuccessFrame) String() string {
	return fmt.Sprintf("[auth_success data=%q]", op.Data)
}

type authChallengeFrame struct {
	Data []byte
}

func (op *authChallengeFrame) String() string {
	return fmt.Sprintf("[auth_challenge data=%q]", op.Data)
}
