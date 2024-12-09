package session

import (
	"fmt"
	"github.com/gocql/gocql/protocol"
)

type ErrProtocol struct{ error }

type BatchType byte

func NewErrProtocol(format string, args ...interface{}) error {
	return ErrProtocol{fmt.Errorf(format, args...)}
}

type ColumnInfo struct {
	Keyspace string
	Table    string
	Name     string
	TypeInfo protocol.TypeInfo
}

func (c ColumnInfo) String() string {
	return fmt.Sprintf("[column keyspace=%s table=%s name=%s type=%v]", c.Keyspace, c.Table, c.Name, c.TypeInfo)
}
