package gocql

// RetryStatement allows us to define whether or not a query should be retried
// or aborted, and can also provide a new consistency level for the retry

type RetryStatementType int

const (
	UnknownRetryStatementType RetryStatementType = iota
	Abort
	Retry
)

// RetryStatement is a struct representing an action on an attempt to retry a
// query
type RetryStatement struct {
	Type             RetryStatementType
	ConsistencyLevel Consistency
}

func NewRetryStatement(level Consistency) *RetryStatement {
	return &RetryStatement{
		Type:             Retry,
		ConsistencyLevel: level,
	}
}

func NewAbortRetryStatement() *RetryStatement {
	return &RetryStatement{
		Type: Abort,
	}
}
