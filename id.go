package wkafka

import (
	"fmt"
	"os"
	"path/filepath"
)

var (
	DefaultClientID string
	idProgname      string
	idHostname      string
)

func init() {
	idProgname = filepath.Base(os.Args[0])
	idHostname, _ = os.Hostname()

	DefaultClientID = fmt.Sprintf("%s@%s", idProgname, idHostname)
}
