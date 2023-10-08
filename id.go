package wkafka

import (
	"fmt"
	"os"
	"path/filepath"
)

var DefaultClientID string

func init() {
	progname := filepath.Base(os.Args[0])
	hostname, _ := os.Hostname()

	DefaultClientID = fmt.Sprintf("%s@%s", progname, hostname)
}
