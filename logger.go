package gocql

import (
	"io/ioutil"
	"log"
)

type stdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}

var Logger stdLogger = log.New(ioutil.Discard, "", log.LstdFlags)
