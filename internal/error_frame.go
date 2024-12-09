package internal

import "fmt"

type ErrorFrame struct {
	FrameHeader

	code    int
	message string
}

func (e ErrorFrame) Code() int {
	return e.code
}

func (e ErrorFrame) Message() string {
	return e.message
}

func (e ErrorFrame) Error() string {
	return e.Message()
}

func (e ErrorFrame) String() string {
	return fmt.Sprintf("[error code=%x message=%q]", e.code, e.message)
}
