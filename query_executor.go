package gocql

import (
	"time"
)

type ExecutableQuery interface {
	execute(conn *Conn) *Iter
	attempt(keyspace string, end, start time.Time, iter *Iter)
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

	qry.attempt(q.pool.keyspace, end, start, iter)

	return iter
}

func (q *queryExecutor) executeQuery(qry ExecutableQuery) (*Iter, error) {
	rt := qry.retryPolicy()
	hostIter := q.policy.Pick(qry)

	var iter *Iter
	hostResponse := hostIter()
	for hostResponse != nil {
		host := hostResponse.Info()
		if host == nil || !host.IsUp() {
			hostResponse = hostIter()
			continue
		}

		pool, ok := q.pool.getPool(host)
		if !ok {
			hostResponse = hostIter()
			continue
		}

		conn := pool.Pick()
		if conn == nil {
			hostResponse = hostIter()
			continue
		}

		iter = q.attemptQuery(qry, conn)

		// Update host
		hostResponse.Mark(iter.err)

		if iter.err == nil {
			// Exit for loop if the query was successful
			iter.host = host
			return iter, nil
		}

		if rt == nil {
			break
		}
		shouldRetry, nextHost := rt.Attempt(qry, iter.err)
		if !shouldRetry {
			break
		}
		if nextHost == false {
			// Do not iter over hosts
			continue
		}
		hostResponse = hostIter()
	}

	if iter == nil {
		return nil, ErrNoConnections
	}

	return iter, nil
}
