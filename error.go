package wkafka

import (
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrNotImplemented = fmt.Errorf("not implemented")
	ErrClientClosed   = fmt.Errorf("client closed")
	ErrNilData        = fmt.Errorf("nil data")
	// ErrSkip is use to skip message in the PreCheck hook.
	ErrSkip = fmt.Errorf("skip message")
)

func wrapErr(r *kgo.Record, err error) error {
	return fmt.Errorf("message error - topic: %q, partition: %d, offset: %d, key: `%s`, headers: `%s` value: `%s`: %w",
		r.Topic, r.Partition, r.Offset, r.Key, stringHeader(r.Headers), r.Value, err,
	)
}

func stringHeader(headers []Header) string {
	var str strings.Builder
	str.WriteString("{")
	for i, header := range headers {
		str.WriteString(fmt.Sprintf("%q: %q", header.Key, header.Value))

		if i == len(headers)-1 {
			continue
		}

		str.WriteString(",")
	}

	str.WriteString("}")

	return str.String()
}
