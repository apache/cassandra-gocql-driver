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

var ErrRetriesExhausted = errors.New("number of retries exhausted")

type retryPolicyWrapper struct {
	p   RetryPolicy
	ctx context.Context
}

func (w *retryPolicyWrapper) Attempt(rq RetryableQuery) error {
	if w.ctx != nil && w.ctx.Err() != nil {
		return w.ctx.Err()
	}
	if w.p == nil {
		return ErrRetriesExhausted
	}
	if !w.p.Attempt(rq) {
		return ErrRetriesExhausted
	}
	return nil
}

func (w *retryPolicyWrapper) GetRetryType(err error) (RetryType, error) {
	// in case there is no error, ignore the policy
	if err == nil {
		return Ignore, nil
	}
	if w.ctx != nil && w.ctx.Err() != nil {
		return Rethrow, w.ctx.Err()
	}
	if w.p == nil {
		// this method is only executed in case the query failed
		// without a retry policy executeQuery should return an error
		return Rethrow, err
	}
	return w.p.GetRetryType(err), nil
}

func (q *queryExecutor) executeQuery(qry ExecutableQuery) (*Iter, error) {
	rp := &retryPolicyWrapper{p: qry.retryPolicy(), ctx: qry.GetContext()}
	hostIter := q.policy.Pick(qry)

	var iter *Iter
	for hostResponse := hostIter(); hostResponse != nil; hostResponse = hostIter() {
		host := hostResponse.Info()
		if host == nil || !host.IsUp() {
			continue
		}

		pool, ok := q.pool.getPool(host)
		if !ok {
			continue
		}

		conn := pool.Pick()
		if conn == nil {
			continue
		}

		iter = q.attemptQuery(qry, conn)
		// Update host
		hostResponse.Mark(iter.err)

		// note host the query was issued against
		iter.host = host

		// exit for loop if the query was successful
		if iter.err == nil {
			return iter, nil
		}

		var retryType RetryType
		retryType, iter.err = rp.GetRetryType(iter.err)
		switch retryType {
		case Retry:
			var err error
			for err = rp.Attempt(qry); err == nil; err = rp.Attempt(qry) {
				iter = q.attemptQuery(qry, conn)
				hostResponse.Mark(iter.err)
				if iter.err == nil {
					iter.host = host
					return iter, nil
				}
				if rt, _ := rp.GetRetryType(iter.err); rt != Retry {
					break
				}
			}
			iter.err = err
		case Rethrow:
			return nil, iter.err
		case Ignore:
			return iter, nil
		case RetryNextHost:
		default:
		}

		if err := rp.Attempt(qry); err != nil {
			iter.err = err
			break
		}
	}

	if iter == nil {
		return nil, ErrNoConnections
	}

	return iter, iter.err
}
