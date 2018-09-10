package gocql

import (
	"sync"
	"time"
)

type ExecutableQuery interface {
	execute(conn *Conn) *Iter
	attempt(keyspace string, end, start time.Time, iter *Iter, host *HostInfo)
	retryPolicy() RetryPolicy
	speculativeExecutionPolicy() SpeculativeExecutionPolicy
	GetRoutingKey() ([]byte, error)
	Keyspace() string
	Cancel()
	IsIdempotent() bool
	RetryableQuery
}

type queryExecutor struct {
	pool   *policyConnPool
	policy HostSelectionPolicy
	specWG sync.WaitGroup
}

func (q *queryExecutor) attemptQuery(qry ExecutableQuery, conn *Conn) *Iter {
	start := time.Now()
	iter := qry.execute(conn)
	end := time.Now()

	qry.attempt(q.pool.keyspace, end, start, iter, conn.host)

	return iter
}

func (q *queryExecutor) executeQuery(qry ExecutableQuery) (*Iter, error) {

	// check whether the query execution set to speculative
	// to make sure, we check if the policy is NonSpeculative or marked as non-idempotent, if it is
	// run normal, otherwise, runs speculative.
	_, nonSpeculativeExecution := qry.speculativeExecutionPolicy().(NonSpeculativeExecution)
	if nonSpeculativeExecution || !qry.IsIdempotent() {
		return q.executeNormalQuery(qry)
	} else {
		return q.executeSpeculativeQuery(qry)
	}
}

func (q *queryExecutor) executeNormalQuery(qry ExecutableQuery) (*Iter, error) {

	var res queryResponse

	results := make(chan queryResponse)
	defer close(results)

	hostIter := q.policy.Pick(qry)
	for selectedHost := hostIter(); selectedHost != nil; selectedHost = hostIter() {
		host := selectedHost.Info()
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

		// store the result
		q.specWG.Add(1)
		go q.runAndStore(qry, conn, selectedHost, results, nil)

		// fetch from the channel whatever is there
		res = <-results

		if res.retryType == RetryNextHost {
			continue
		}

		return res.iter, res.err

	}

	if res.iter == nil {
		return nil, ErrNoConnections
	}

	return res.iter, nil
}

func (q *queryExecutor) executeSpeculativeQuery(qry ExecutableQuery) (*Iter, error) {
	hostIter := q.policy.Pick(qry)
	sp := qry.speculativeExecutionPolicy()

	results := make(chan queryResponse)

	stopExecutions := make(chan struct{})
	defer close(stopExecutions)

	var (
		res             queryResponse
		specExecCounter int
	)
	for selectedHost := hostIter(); selectedHost != nil && specExecCounter < sp.Executions(); selectedHost = hostIter() {
		host := selectedHost.Info()
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

		// if it's not the first attempt, pause
		if specExecCounter > 0 {
			<-time.After(sp.Delay())
		}

		// push the results into a channel
		q.specWG.Add(1)
		go q.runAndStore(qry, conn, selectedHost, results, stopExecutions)
		specExecCounter += 1
	}

	// defer cleaning
	go func() {
		q.specWG.Wait()
		close(results)
	}()

	// check the results
	for res := range results {
		if res.retryType == RetryNextHost {
			continue
		}

		if res.err == nil {
			return res.iter, res.err
		}
	}

	if res.iter == nil {
		return nil, ErrNoConnections
	}

	return res.iter, nil
}

func (q *queryExecutor) runAndStore(qry ExecutableQuery, conn *Conn, selectedHost SelectedHost, results chan queryResponse, stopExecutions chan struct{}) {

	rt := qry.retryPolicy()
	host := selectedHost.Info()

	// Run the query
	iter := q.attemptQuery(qry, conn)
	iter.host = host
	// Update host
	selectedHost.Mark(iter.err)

	// Handle the wait group
	defer q.specWG.Done()

	// Exit if the query was successful
	// or no retry policy defined
	if rt == nil || iter.err == nil {
		results <- queryResponse{iter: iter}
		return
	}

	// If query is unsuccessful, use RetryPolicy to retry
	select {
	case <-stopExecutions:
		// We're done, stop everyone else
		qry.Cancel()
	default:
		for rt.Attempt(qry) {
			switch rt.GetRetryType(iter.err) {
			case Retry:
				iter = q.attemptQuery(qry, conn)
				selectedHost.Mark(iter.err)
				if iter.err == nil {
					iter.host = host
					results <- queryResponse{iter: iter}
					return
				}
			case Rethrow:
				results <- queryResponse{err: iter.err}
				return
			case Ignore:
				results <- queryResponse{iter: iter}
				return
			case RetryNextHost:
				results <- queryResponse{retryType: RetryNextHost}
				return
			default:
				results <- queryResponse{iter: iter, err: iter.err}
				return
			}
		}
	}
}

type queryResponse struct {
	iter      *Iter
	err       error
	retryType RetryType
}
