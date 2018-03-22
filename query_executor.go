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

	iter := &Iter{err: new(Error)}
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

		for iter.err != nil && rt.Attempt(qry) {
			iter = q.attemptQuery(qry, conn)
			Logger.Print(iter.err)
			Logger.Print(qry.Attempts())
			if rt.GetRetryType(iter.err) != Retry {
				break
			}
			if rt == nil || !rt.Attempt(qry) {
				goto exit
			}

		}
		// Update host
		hostResponse.Mark(iter.err)

		// Exit for loop if the query was successful
		if iter.err == nil {
			iter.host = host
			return iter, nil
		}

		if rt == nil || !rt.Attempt(qry) {
			// What do here? Should we just return an error here?
			break
		}
	}

	exit:
	if iter == nil {
		return nil, ErrNoConnections
	}

	return iter, nil
}
