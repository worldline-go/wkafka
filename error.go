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
	errNotImplemented   = fmt.Errorf("not implemented")
	errClientClosed     = fmt.Errorf("client closed")
	errPartitionRevoked = fmt.Errorf("partition revoked")

	// ErrSkip is use to skip message in the PreCheck hook or Decode function.
	ErrSkip = fmt.Errorf("skip message")
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
