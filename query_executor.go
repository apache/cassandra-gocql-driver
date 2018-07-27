package gocql

import (
	"context"
	"errors"
	"time"
)

type ExecutableQuery interface {
	execute(conn *Conn) *Iter
	attempt(keyspace string, end, start time.Time, iter *Iter, host *HostInfo)
	retryPolicy() RetryPolicy
	GetRoutingKey() ([]byte, error)
	Keyspace() string
	RetryableQuery
}

type queryExecutor struct {
	pool   *policyConnPool
	policy HostSelectionPolicy
}

func (q *queryExecutor) attemptQuery(qry ExecutableQuery, conn *Conn) *Iter {
	start := time.Now()
	iter := qry.execute(conn)
	end := time.Now()

	qry.attempt(q.pool.keyspace, end, start, iter, conn.host)

	return iter
}

// ErrUnknownRetryType is returned if the retry policy returns a retry type
// unknown to the query executor.
var ErrUnknownRetryType = errors.New("unknown retry type returned by retry policy")

type retryPolicyWrapper struct {
	p   RetryPolicy
	ctx context.Context
}

// attempt is used by the query executor to determine with retrying a query.
// It consults the query context and the query's retry policy.
func (w *retryPolicyWrapper) attempt(rq RetryableQuery, err error) (RetryType, error) {
	ctx := rq.GetContext()
	if ctx != nil && ctx.Err() != nil { // context on query expired or was canceled, bail
		return Rethrow, ctx.Err()
	}
	if w.p == nil { // bubble error to the caller if there is no retry policy
		return Rethrow, err
	}
	if w.p.Attempt(rq) {
		return w.p.GetRetryType(err), nil
	}
	return w.p.GetRetryType(err), err
}

func (q *queryExecutor) executeQuery(qry ExecutableQuery) (*Iter, error) {
	retryPolicyWrapper := &retryPolicyWrapper{p: qry.retryPolicy(), ctx: qry.GetContext()}
	hostIter := q.policy.Pick(qry)
	var iter *Iter

outer:
	for hostResponse := hostIter(); hostResponse != nil; hostResponse = hostIter() {
		host := hostResponse.Info()
		if host == nil || !host.IsUp() {
			continue
		}
		hostPool, ok := q.pool.getPool(host)
		if !ok {
			continue
		}

		conn := hostPool.Pick()
		if conn == nil {
			continue
		}
	inner:
		for {
			iter = q.attemptQuery(qry, conn)
			// Update host
			hostResponse.Mark(iter.err)

			// note host the query was issued against
			iter.host = host

			// exit if the query was successful
			if iter.err == nil {
				return iter, nil
			}

			// consult retry policy on how to proceed
			var retryType RetryType
			retryType, iter.err = retryPolicyWrapper.attempt(qry, iter.err)
			switch retryType {
			case Retry:
				continue inner
			case Rethrow:
				return nil, iter.err
			case Ignore:
				return iter, nil
			case RetryNextHost:
				continue outer
			default:
				return nil, ErrUnknownRetryType
			}
		}
	}

	// if we reach this point, there is no host in the pool
	return nil, ErrNoConnections
}
