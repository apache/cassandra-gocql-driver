// +build !go1.2

package gocql

import (
	"log"
	"time"
)

func (c *Conn) setKeepalive(d time.Duration) error {
	log.Println("WARN: KeepAlive provided but not supported on Go < 1.2")
	return nil
}
