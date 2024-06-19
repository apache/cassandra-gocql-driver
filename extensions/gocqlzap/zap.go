package gocqlzap

import (
	"github.com/gocql/gocql"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const DefaultName = "gocql"

type Logger interface {
	gocql.AdvancedLogger
	ZapLogger() *zap.Logger
	Name() string
}

type Options struct {
	LogLevel zapcore.Level
}

type logger struct {
	zapLogger *zap.Logger
}

// NewZapLogger creates a new zap based logger with the logger name set to DefaultName
func NewZapLogger(l *zap.Logger, opts Options) Logger {
	return &logger{zapLogger: l.Named(DefaultName)}
}

// NewUnnamedZapLogger doesn't set the logger name so the user can set the name of the logger
// before providing it to this function (or just leave it unset)
func NewUnnamedZapLogger(l *zap.Logger) Logger {
	return &logger{zapLogger: l}
}

func (rec *logger) ZapLogger() *zap.Logger {
	return rec.zapLogger
}

func (rec *logger) Name() string {
	return rec.zapLogger.Name()
}

func (rec *logger) log(fields []gocql.LogField) *zap.Logger {
	childLogger := rec.zapLogger
	for _, field := range fields {
		childLogger = childLogger.WithLazy(zap.Any(field.Name, field.Value))
	}
	return childLogger
}

func (rec *logger) Error(msg string, fields ...gocql.LogField) {
	rec.log(fields).Error(msg)
}

func (rec *logger) Warning(msg string, fields ...gocql.LogField) {
	rec.log(fields).Warn(msg)
}

func (rec *logger) Info(msg string, fields ...gocql.LogField) {
	rec.log(fields).Info(msg)
}

func (rec *logger) Debug(msg string, fields ...gocql.LogField) {
	rec.log(fields).Debug(msg)
}
