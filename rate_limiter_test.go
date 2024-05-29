package gocql

import (
	"fmt"
	"sync"
	"testing"
)

const queries = 100

const skipRateLimiterTestMsg = "Skipping rate limiter test, due to limit of simultaneously alive goroutines. Should be tested locally"

func TestRateLimiter50k(t *testing.T) {
	t.Skip(skipRateLimiterTestMsg)
	fmt.Println("Running rate limiter test with 50_000 workers")
	RunRateLimiterTest(t, 50_000)
}

func TestRateLimiter100k(t *testing.T) {
	t.Skip(skipRateLimiterTestMsg)
	fmt.Println("Running rate limiter test with 100_000 workers")
	RunRateLimiterTest(t, 100_000)
}

func TestRateLimiter200k(t *testing.T) {
	t.Skip(skipRateLimiterTestMsg)
	fmt.Println("Running rate limiter test with 200_000 workers")
	RunRateLimiterTest(t, 200_000)
}

func RunRateLimiterTest(t *testing.T, workerCount int) {
	cluster := createCluster()
	cluster.RateLimiterConfig = &RateLimiterConfig{
		rate:  300000,
		burst: 100,
	}

	session := createSessionFromCluster(cluster, t)
	defer session.Close()

	execRelease(session.Query("drop keyspace if exists pargettest"))
	execRelease(session.Query("create keyspace pargettest with replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}"))
	execRelease(session.Query("drop table if exists pargettest.test"))
	execRelease(session.Query("create table pargettest.test (a text, b int, primary key(a))"))
	execRelease(session.Query("insert into pargettest.test (a, b) values ( 'a', 1)"))

	var wg sync.WaitGroup

	for i := 1; i <= workerCount; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			for j := 0; j < queries; j++ {
				iterRelease(session.Query("select * from pargettest.test where a='a'"))
			}
		}()
	}

	wg.Wait()
}

func iterRelease(query *Query) {
	_, err := query.Iter().SliceMap()
	if err != nil {
		println(err.Error())
		panic(err)
	}
	query.Release()
}

func execRelease(query *Query) {
	if err := query.Exec(); err != nil {
		println(err.Error())
		panic(err)
	}
	query.Release()
}
