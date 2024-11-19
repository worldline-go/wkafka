package wkafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	errNotImplemented   = errors.New("not implemented")
	errClientClosed     = errors.New("client closed")
	errPartitionRevoked = errors.New("partition revoked")

	// ErrSkip is use to skip message in the PreCheck hook or Decode function.
	ErrSkip = errors.New("skip message")
	// ErrDLQ use with callback function to send message to DLQ topic.
	// Prefer to use WrapErrDLQ to wrap error.
	ErrDLQ = errors.New("error DLQ")
)

// DLQError is use with callback function to send message to DLQ topic.
type DLQError struct {
	// Err is default error to add in header.
	// If not setted, header will just show "DLQ indexed error"
	Err error
	// Indexes to use send specific batch index to DLQ.
	// If index's error is nil, default error is used.
	Indexes map[int]error
}

func WrapErrDLQ(err error) *DLQError {
	return &DLQError{Err: err}
}

func (e *DLQError) Error() string {
	if e.Err != nil {
		return e.Err.Error()
	}

	return "DLQ indexed error"
}

// IsDQLError check if error is DLQ error and return it.
func IsDQLError(err error) (*DLQError, bool) {
	var errDLQIndexed *DLQError

	ok := errors.As(err, &errDLQIndexed)
	if ok {
		return errDLQIndexed, true
	}

	if errors.Is(err, ErrDLQ) {
		return &DLQError{Err: err}, true
	}

	return nil, false
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

func errorOffsetList(records []*kgo.Record) string {
	topicPartitionOffsets := make(map[string]map[int32][]int)

	// Organizing the records by topic and partition
	for _, record := range records {
		if _, ok := topicPartitionOffsets[record.Topic]; !ok {
			topicPartitionOffsets[record.Topic] = make(map[int32][]int)
		}
		topicPartitionOffsets[record.Topic][record.Partition] = append(topicPartitionOffsets[record.Topic][record.Partition], int(record.Offset))
	}

	// Formatting the offsets
	result := make(map[string]map[string]string)
	for topic, partitions := range topicPartitionOffsets {
		result[topic] = make(map[string]string)
		for partition, offsets := range partitions {
			sort.Ints(offsets) // Ensure offsets are sorted
			result[topic][strconv.Itoa(int(partition))] = formatOffsets(offsets)
		}
	}

	jsonResult, _ := json.Marshal(result)
	return string(jsonResult)
}

// formatOffsets formats a slice of ints into a string, grouping consecutive numbers.
func formatOffsets(offsets []int) string {
	if len(offsets) == 0 {
		return ""
	}

	var result string
	start := offsets[0]
	end := start

	for i := 1; i < len(offsets); i++ {
		if offsets[i] == end+1 {
			end = offsets[i]
		} else {
			result += formatRange(start, end) + ","
			start = offsets[i]
			end = start
		}
	}
	result += formatRange(start, end)
	return result
}

// formatRange formats a range of numbers into a string.
func formatRange(start, end int) string {
	if start == end {
		return strconv.Itoa(start)
	}
	return strconv.Itoa(start) + "-" + strconv.Itoa(end)
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
