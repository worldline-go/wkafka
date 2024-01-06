package wkafka

import (
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrNotImplemented = fmt.Errorf("not implemented")
	ErrClientClosed   = fmt.Errorf("client closed")
	// ErrSkip is use to skip message in the PreCheck hook or Decode function.
	ErrSkip = fmt.Errorf("skip message")
	// ErrInvalidCompression for producer setting check.
	ErrInvalidCompression = fmt.Errorf("invalid compression")
	// ErrDLQ use with callback function to send message to DLQ topic.
	ErrDLQ = fmt.Errorf("send to DLQ")
)

// DLQIndexedError is use with callback function to send message to DLQ topic with specific index.
type DLQIndexedError struct {
	// Err is default error to add in header.
	Err error
	// Indexes if not empty, use to add error in specific index.
	Indexes map[int]error
}

func (e *DLQIndexedError) Error() string {
	return "DLQ indexed error"
}

// isDQLError check if error is DLQ error and return the original error or error.
func isDQLError(err error) (error, bool) {
	if errors.Is(err, ErrDLQ) {
		return unwrapErr(err), true
	}

	var errDLQIndexed *DLQIndexedError

	ok := errors.As(err, &errDLQIndexed)
	if ok {
		return errDLQIndexed.Err, true
	}

	return err, false
}

func wrapErr(r *kgo.Record, err error, dlq bool) error {
	dlqMsg := ""
	if dlq {
		dlqMsg = "DLQ "
	}

	return fmt.Errorf("%smessage error - topic: %q, partition: %d, offset: %d, key: `%s`, headers: `%s` value: `%s`: %w",
		dlqMsg, r.Topic, r.Partition, r.Offset, r.Key, stringHeader(r.Headers), r.Value, err,
	)
}

func unwrapErr(err error) error {
	return errors.Unwrap(err)
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
