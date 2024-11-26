package gocqlzerolog

import (
	"github.com/gocql/gocql"
	"github.com/rs/zerolog"
)

const DefaultName = "gocql"
const DefaultNameField = "logger"

type Logger interface {
	gocql.AdvancedLogger
	ZerologLogger() zerolog.Logger
}

type logger struct {
	zerologLogger zerolog.Logger
}

// NewZerologLogger creates a new zerolog based logger with a global context containing a field
// with name "logger" and value "gocql", i.e.:
//
//	l.With().Str("logger", "gocql").Logger()
func NewZerologLogger(l zerolog.Logger) Logger {
	return &logger{zerologLogger: l.With().Str(DefaultNameField, DefaultName).Logger()}
}

// NewUnnamedZerologLogger creates a new zerolog based logger without modifying its context like
// NewZerologLogger does.
func NewUnnamedZerologLogger(l zerolog.Logger) Logger {
	return &logger{zerologLogger: l}
}

func (rec *logger) ZerologLogger() zerolog.Logger {
	return rec.zerologLogger
}

func (rec *logger) log(event *zerolog.Event, fields ...gocql.LogField) *zerolog.Event {
	for _, field := range fields {
		event = event.Any(field.Name, field.Value)
	}
	return event
}

func (rec *logger) Error(msg string, fields ...gocql.LogField) {
	rec.log(rec.zerologLogger.Error(), fields...).Msg(msg)
}

func (rec *logger) Warning(msg string, fields ...gocql.LogField) {
	rec.log(rec.zerologLogger.Warn(), fields...).Msg(msg)
}

func (rec *logger) Info(msg string, fields ...gocql.LogField) {
	rec.log(rec.zerologLogger.Info(), fields...).Msg(msg)
}

func (rec *logger) Debug(msg string, fields ...gocql.LogField) {
	rec.log(rec.zerologLogger.Debug(), fields...).Msg(msg)
}
