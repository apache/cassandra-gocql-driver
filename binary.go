package gocql

import (
	"errors"
	"net"
)

const (
	protoRequest  byte = 0x02
	protoResponse byte = 0x82

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

	headerSize = 8
)

var ErrInvalid = errors.New("invalid response")

type buffer []byte

func (b *buffer) writeInt(v int32) {
	p := b.grow(4)
	(*b)[p] = byte(v >> 24)
	(*b)[p+1] = byte(v >> 16)
	(*b)[p+2] = byte(v >> 8)
	(*b)[p+3] = byte(v)
}

func (b *buffer) writeShort(v uint16) {
	p := b.grow(2)
	(*b)[p] = byte(v >> 8)
	(*b)[p+1] = byte(v)
}

func (b *buffer) writeString(v string) {
	b.writeShort(uint16(len(v)))
	p := b.grow(len(v))
	copy((*b)[p:], v)
}

func (b *buffer) writeLongString(v string) {
	b.writeInt(int32(len(v)))
	p := b.grow(len(v))
	copy((*b)[p:], v)
}

func (b *buffer) writeUUID() {
}

func (b *buffer) writeStringList(v []string) {
	b.writeShort(uint16(len(v)))
	for i := range v {
		b.writeString(v[i])
	}
}

func (b *buffer) writeByte(v byte) {
	p := b.grow(1)
	(*b)[p] = v
}

func (b *buffer) writeBytes(v []byte) {
	if v == nil {
		b.writeInt(-1)
		return
	}
	b.writeInt(int32(len(v)))
	p := b.grow(len(v))
	copy((*b)[p:], v)
}

func (b *buffer) writeShortBytes(v []byte) {
	b.writeShort(uint16(len(v)))
	p := b.grow(len(v))
	copy((*b)[p:], v)
}

func (b *buffer) writeInet(ip net.IP, port int) {
	p := b.grow(1 + len(ip))
	(*b)[p] = byte(len(ip))
	copy((*b)[p+1:], ip)
	b.writeInt(int32(port))
}

func (b *buffer) writeConsistency() {
}

func (b *buffer) writeStringMap(v map[string]string) {
	b.writeShort(uint16(len(v)))
	for key, value := range v {
		b.writeString(key)
		b.writeString(value)
	}
}

func (b *buffer) writeStringMultimap(v map[string][]string) {
	b.writeShort(uint16(len(v)))
	for key, values := range v {
		b.writeString(key)
		b.writeStringList(values)
	}
}

func (b *buffer) setHeader(version, flags, stream, opcode uint8) {
	(*b)[0] = version
	(*b)[1] = flags
	(*b)[2] = stream
	(*b)[3] = opcode
}

func (b *buffer) setLength(length int) {
	(*b)[4] = byte(length >> 24)
	(*b)[5] = byte(length >> 16)
	(*b)[6] = byte(length >> 8)
	(*b)[7] = byte(length)
}

func (b *buffer) Length() int {
	return int((*b)[4])<<24 | int((*b)[5])<<16 | int((*b)[6])<<8 | int((*b)[7])
}

func (b *buffer) grow(n int) int {
	if len(*b)+n >= cap(*b) {
		buf := make(buffer, len(*b), len(*b)*2+n)
		copy(buf, *b)
		*b = buf
	}
	p := len(*b)
	*b = (*b)[:p+n]
	return p
}

func (b *buffer) skipHeader() {
	*b = (*b)[headerSize:]
}

func (b *buffer) readInt() int {
	if len(*b) < 4 {
		panic(ErrInvalid)
	}
	v := int((*b)[0])<<24 | int((*b)[1])<<16 | int((*b)[2])<<8 | int((*b)[3])
	*b = (*b)[4:]
	return v
}

func (b *buffer) readShort() uint16 {
	if len(*b) < 2 {
		panic(ErrInvalid)
	}
	v := uint16((*b)[0])<<8 | uint16((*b)[1])
	*b = (*b)[2:]
	return v
}

func (b *buffer) readString() string {
	n := int(b.readShort())
	if len(*b) < n {
		panic(ErrInvalid)
	}
	v := string((*b)[:n])
	*b = (*b)[n:]
	return v
}

func (b *buffer) readBytes() []byte {
	n := b.readInt()
	if n < 0 {
		return nil
	}
	if len(*b) < n {
		panic(ErrInvalid)
	}
	v := (*b)[:n]
	*b = (*b)[n:]
	return v
}

func (b *buffer) readShortBytes() []byte {
	n := int(b.readShort())
	if len(*b) < n {
		panic(ErrInvalid)
	}
	v := (*b)[:n]
	*b = (*b)[n:]
	return v
}

func (b *buffer) readTypeInfo() *TypeInfo {
	x := b.readShort()
	typ := &TypeInfo{Type: Type(x)}
	switch typ.Type {
	case TypeCustom:
		typ.Custom = b.readString()
	case TypeMap:
		typ.Key = b.readTypeInfo()
		fallthrough
	case TypeList, TypeSet:
		typ.Value = b.readTypeInfo()
	}
	return typ
}

func (b *buffer) readMetaData() []columnInfo {
	flags := b.readInt()
	numColumns := b.readInt()
	globalKeyspace := ""
	globalTable := ""
	if flags&1 != 0 {
		globalKeyspace = b.readString()
		globalTable = b.readString()
	}
	info := make([]columnInfo, numColumns)
	for i := 0; i < numColumns; i++ {
		info[i].Keyspace = globalKeyspace
		info[i].Table = globalTable
		if flags&1 == 0 {
			info[i].Keyspace = b.readString()
			info[i].Table = b.readString()
		}
		info[i].Name = b.readString()
		info[i].TypeInfo = b.readTypeInfo()
	}
	return info
}
