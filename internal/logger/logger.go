package logger

import (
	"bytes"
	"fmt"
	"log"
)

type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

type NopLogger struct{}

func (n NopLogger) Print(_ ...interface{}) {}

func (n NopLogger) Printf(_ string, _ ...interface{}) {}

func (n NopLogger) Println(_ ...interface{}) {}

type TestLogger struct {
	capture bytes.Buffer
}

func (l *TestLogger) Print(v ...interface{})                 { fmt.Fprint(&l.capture, v...) }
func (l *TestLogger) Printf(format string, v ...interface{}) { fmt.Fprintf(&l.capture, format, v...) }
func (l *TestLogger) Println(v ...interface{})               { fmt.Fprintln(&l.capture, v...) }
func (l *TestLogger) String() string                         { return l.capture.String() }

type DefaultLogger struct{}

func (l *DefaultLogger) Print(v ...interface{})                 { log.Print(v...) }
func (l *DefaultLogger) Printf(format string, v ...interface{}) { log.Printf(format, v...) }
func (l *DefaultLogger) Println(v ...interface{})               { log.Println(v...) }

// Logger for logging messages.
// Deprecated: Use ClusterConfig.Logger instead.
var Logger StdLogger = &DefaultLogger{}
