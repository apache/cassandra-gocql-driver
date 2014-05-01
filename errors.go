package gocql

const (
	errServer        = 0x0000
	errProtocol      = 0x000A
	errCredentials   = 0x0100
	errUnavailable   = 0x1000
	errOverloaded    = 0x1001
	errBootstrapping = 0x1002
	errTruncate      = 0x1003
	errWriteTimeout  = 0x1100
	errReadTimeout   = 0x1200
	errSyntax        = 0x2000
	errUnauthorized  = 0x2100
	errInvalid       = 0x2200
	errConfig        = 0x2300
	errAlreadyExists = 0x2400
	errUnprepared    = 0x2500
)

type errorFrame interface {
	Code() int
	Message() string
	Error() string
}

type errorResponse struct {
	code    int
	message string
}

func (e errorResponse) Code() int {
	return e.code
}

func (e errorResponse) Message() string {
	return e.message
}

func (e errorResponse) Error() string {
	return e.Message()
}

type errRespUnavailable struct {
	errorResponse
	Consistency Consistency
	Required    int
	Alive       int
}

type errRespWriteTimeout struct {
	errorResponse
	Consistency Consistency
	Received    int
	BlockFor    int
	WriteType   string
}

type errRespReadTimeout struct {
	errorResponse
	Consistency Consistency
	Received    int
	BlockFor    int
	DataPresent byte
}

type errRespAlreadyExists struct {
	errorResponse
	Keyspace string
	Table    string
}

type errRespUnprepared struct {
	errorResponse
	StatementId []byte
}
