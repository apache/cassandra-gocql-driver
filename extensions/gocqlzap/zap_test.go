package gocqlzap

import (
	"bytes"
	"github.com/gocql/gocql"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"io"
	"strings"
	"testing"
)

const logLineEnding = "%%%\n%%%"

func NewCustomLogger(pipeTo io.Writer) zapcore.Core {
	cfg := zap.NewProductionEncoderConfig()
	cfg.LineEnding = logLineEnding
	return zapcore.NewCore(
		zapcore.NewConsoleEncoder(cfg),
		zapcore.AddSync(pipeTo),
		zapcore.DebugLevel,
	)
}

func TestGocqlZapLog(t *testing.T) {
	b := &bytes.Buffer{}
	logger := zap.New(NewCustomLogger(b))
	clusterCfg := gocql.NewCluster("0.0.0.1")
	clusterCfg.StructuredLogger = NewZapLogger(logger)
	clusterCfg.LegacyLogLevel = gocql.LogLevelDebug
	clusterCfg.ProtoVersion = 4
	session, err := clusterCfg.CreateSession()
	if err == nil {
		session.Close()
		t.Fatal("expected error creating session")
	}
	err = logger.Sync()
	if err != nil {
		t.Fatal("logger sync failed")
	}
	logOutput := strings.Split(b.String(), logLineEnding)
	found := false
	for _, logEntry := range logOutput {
		if len(logEntry) == 0 {
			continue
		}
		if !strings.Contains(logEntry, "info\tgocql\tunable to dial control conn %s:%v (%s): %v\t{\"host_addr\": "+
			"\"0.0.0.1\", \"port\": 9042, \"host_id\": \"\", \"err\": \"dial tcp 0.0.0.1:9042:") {
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
