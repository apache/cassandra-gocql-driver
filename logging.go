package gocql

import (
	"os"

	"log"
)

var (
	// Logger to use, should be set before initialising the cluster as it is not
	// threadsafe for concurrent access. The default logger will output messages
	// which are <= LOG_ERROR to Stderr with the prefix 'gocql'.
	Log = &LogWrapper{
		Threshold: LOG_ERROR,
		Output:    log.New(os.Stderr, "gocql", log.LstdFlags),
	}
)

type LogLevel int

const (
	LOG_EMERGENCY LogLevel = iota
	LOG_ALERT
	LOG_CRITICAL
	LOG_ERROR
	LOG_WANRING
	LOG_NOTICE
	LOG_INFO
	LOG_DEBUG
)

type Logger interface {
	Emerg(m string) error
	Alert(m string) error
	Crit(m string) error
	Err(m string) error
	Warning(m string) error
	Notice(m string) error
	Info(m string) error
	Debug(m string) error
}

// Implements a Logger by wrapping the logger defined in the log pacakge.
// Only outputs logs for which are below a certain level, default is LOG_ERROR
type LogWrapper struct {
	// Only logs which level are equal to or lower than Threshold will be sent.
	Threshold LogLevel

	// The log target.
	Output *log.Logger
}

func (l *LogWrapper) Emerg(m string) error {
	if LOG_EMERGENCY <= l.Threshold {
		log.Printf("EMERG: %s\n", m)
	}

	return nil
}

func (l *LogWrapper) Alert(m string) error {
	if LOG_ALERT <= l.Threshold {
		log.Printf("ALERT: %s\n", m)
	}

	return nil
}

func (l *LogWrapper) Crit(m string) error {
	if LOG_CRITICAL <= l.Threshold {
		log.Printf("CRIT: %s\n", m)
	}

	return nil
}

func (l *LogWrapper) Err(m string) error {
	if LOG_ERROR <= l.Threshold {
		log.Printf("ERR: %s\n", m)
	}

	return nil
}

func (l *LogWrapper) Warning(m string) error {
	if LOG_WANRING <= l.Threshold {
		log.Printf("WARNING: %s\n", m)
	}

	return nil
}

func (l *LogWrapper) Notice(m string) error {
	if LOG_NOTICE <= l.Threshold {
		log.Printf("NOTICE: %s\n", m)
	}

	return nil
}

func (l *LogWrapper) Info(m string) error {
	if LOG_INFO <= l.Threshold {
		log.Printf("INFO: %s\n", m)
	}

	return nil
}

func (l *LogWrapper) Debug(m string) error {
	if LOG_DEBUG <= l.Threshold {
		log.Printf("DEBUG: %s\n", m)
	}

	return nil
}
