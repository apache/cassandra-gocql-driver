package gocql

import (
	"context"
	"github.com/opentracing/opentracing-go"
	"time"
)

type QueryTracer struct{}

type spanStartOpt struct {
	startTime time.Time
}

func (sso spanStartOpt) Apply(o *opentracing.StartSpanOptions) {
	o.StartTime = sso.startTime
}

func (qt *QueryTracer) ObserveQuery(ctx context.Context, query ObservedQuery) {
	if span, _ := opentracing.StartSpanFromContext(ctx, query.Statement, spanStartOpt{startTime: query.Start}); span != nil {
		defer span.FinishWithOptions(opentracing.FinishOptions{FinishTime: query.End})
		// constants
		span.SetTag("span.kind", "client")
		span.SetTag("component", "go-cassandra")
		span.SetTag("db.type", "cassandra")

		// conditional error
		if query.Err != nil {
			span.SetTag("db.err", query.Err)
			span.SetTag("error", true)
		}

		// query info
		span.SetTag("db.instance", query.Keyspace)
		span.SetTag("db.statement", query.Statement)
		span.SetTag("db.rows", query.Rows)
		span.SetTag("db.attempt", query.Attempt)

		// connection info
		if query.Host != nil {
			span.SetTag("host", query.Host.HostID())
			span.SetTag("host.id", query.Host.HostID())
			span.SetTag("host.dc", query.Host.DataCenter())
			span.SetTag("peer.hostname", query.Host.HostnameAndPort())
			span.SetTag("peer.ip", query.Host.Peer())
			span.SetTag("peer.port", query.Host.Port())
		}
	}
}
