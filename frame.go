// Copyright (c) 2012 The gocql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gocql

import (
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

type frame []byte

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

func (f *frame) setHeader(version, flags, stream, opcode uint8) {
	(*f)[0] = version
	(*f)[1] = flags
	(*f)[2] = stream
	(*f)[3] = opcode
}

func (f *frame) setLength(length int) {
	(*f)[4] = byte(length >> 24)
	(*f)[5] = byte(length >> 16)
	(*f)[6] = byte(length >> 8)
	(*f)[7] = byte(length)
}

func (f *frame) Length() int {
	return int((*f)[4])<<24 | int((*f)[5])<<16 | int((*f)[6])<<8 | int((*f)[7])
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

func (f *frame) skipHeader() {
	*f = (*f)[headerSize:]
}

func (f *frame) readInt() int {
	if len(*f) < 4 {
		panic(ErrProtocol)
	}
	v := int((*f)[0])<<24 | int((*f)[1])<<16 | int((*f)[2])<<8 | int((*f)[3])
	*f = (*f)[4:]
	return v
}

func (f *frame) readShort() uint16 {
	if len(*f) < 2 {
		panic(ErrProtocol)
	}
	v := uint16((*f)[0])<<8 | uint16((*f)[1])
	*f = (*f)[2:]
	return v
}

func (f *frame) readString() string {
	n := int(f.readShort())
	if len(*f) < n {
		panic(ErrProtocol)
	}
	v := string((*f)[:n])
	*f = (*f)[n:]
	return v
}

func (f *frame) readLongString() string {
	n := f.readInt()
	if len(*f) < n {
		panic(ErrProtocol)
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
		panic(ErrProtocol)
	}
	v := (*f)[:n]
	*f = (*f)[n:]
	return v
}

func (f *frame) readShortBytes() []byte {
	n := int(f.readShort())
	if len(*f) < n {
		panic(ErrProtocol)
	}
	v := (*f)[:n]
	*f = (*f)[n:]
	return v
}

func (f *frame) readTypeInfo() *TypeInfo {
	x := f.readShort()
	typ := &TypeInfo{Type: Type(x)}
	switch typ.Type {
	case TypeCustom:
		typ.Custom = f.readString()
	case TypeMap:
		typ.Key = f.readTypeInfo()
		fallthrough
	case TypeList, TypeSet:
		typ.Elem = f.readTypeInfo()
	}
	return typ
}

func (f *frame) readMetaData() []ColumnInfo {
	flags := f.readInt()
	numColumns := f.readInt()
	globalKeyspace := ""
	globalTable := ""
	if flags&1 != 0 {
		globalKeyspace = f.readString()
		globalTable = f.readString()
	}
	info := make([]ColumnInfo, numColumns)
	for i := 0; i < numColumns; i++ {
		info[i].Keyspace = globalKeyspace
		info[i].Table = globalTable
		if flags&1 == 0 {
			info[i].Keyspace = f.readString()
			info[i].Table = f.readString()
		}
		info[i].Name = f.readString()
		info[i].TypeInfo = f.readTypeInfo()
	}
	return info
}

func (f *frame) readErrorFrame() (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok && e == ErrProtocol {
				err = e
				return
			}
			panic(r)
		}
	}()
	f.skipHeader()
	code := f.readInt()
	desc := f.readString()
	return Error{code, desc}
}
