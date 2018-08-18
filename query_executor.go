package gocql

import (
	"errors"
	"time"
)

// ErrUnknownRetryType is returned if the retry policy returns a retry type
// unknown to the query executor.
var ErrUnknownRetryType = errors.New("unknown retry type returned by retry policy")

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

// checkRetryPolicy is used by the query executor to determine how a failed query should be handled.
// It consults the query context and the query's retry policy.
func (q *queryExecutor) checkRetryPolicy(rq ExecutableQuery, err error) (RetryType, error) {
	if ctx := rq.Context(); ctx != nil {
		if ctx.Err() != nil {
			return Rethrow, ctx.Err()
		}
	}
	p := rq.retryPolicy()
	if p == nil {
		return Rethrow, err
	}
	if p.Attempt(rq) {
		return p.GetRetryType(err), nil
	}
	return p.GetRetryType(err), err
}

func (q *queryExecutor) executeQuery(qry ExecutableQuery) (*Iter, error) {
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
			retryType, iter.err = q.checkRetryPolicy(qry, iter.err)
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
