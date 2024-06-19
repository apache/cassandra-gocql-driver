package gocqlzerolog

import (
	"bytes"
	"github.com/gocql/gocql"
	"github.com/rs/zerolog"
	"strings"
	"testing"
)

const logLineEnding = "%%%\n%%%"

func TestGocqlZeroLog(t *testing.T) {
	b := &bytes.Buffer{}
	output := zerolog.ConsoleWriter{Out: b}
	output.NoColor = true
	output.FormatExtra = func(m map[string]interface{}, buffer *bytes.Buffer) error {
		buffer.WriteString(logLineEnding)
		return nil
	}
	logger := zerolog.New(output)
	clusterCfg := gocql.NewCluster("0.0.0.1")
	clusterCfg.StructuredLogger = NewZerologLogger(logger)
	clusterCfg.LegacyLogLevel = gocql.LogLevelDebug
	clusterCfg.ProtoVersion = 4
	session, err := clusterCfg.CreateSession()
	if err == nil {
		session.Close()
		t.Fatal("expected error creating session")
	}
	logOutput := strings.Split(b.String(), logLineEnding+"\n")
	found := false
	for _, logEntry := range logOutput {
		if len(logEntry) == 0 {
			continue
		}
		if !strings.Contains(logEntry, "unable to dial control conn %s:%v (%s): %v err=\"dial tcp 0.0.0.1:9042:") ||
			!strings.Contains(logEntry, "host_addr=0.0.0.1 host_id= logger=gocql port=9042") {
			continue
		} else {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("log output didn't match expectations: ", strings.Join(logOutput, "\n"))
	}
}
