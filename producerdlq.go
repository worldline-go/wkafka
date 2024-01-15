package wkafka

import (
	"context"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// producerDLQ to push failed records to dead letter queue.
//   - err could be ErrDLQIndexed or any other error
func producerDLQ(topic string, clientID []byte, fn func(ctx context.Context, records []*kgo.Record) error) func(ctx context.Context, err *DLQError, records []*kgo.Record) error {
	return func(ctx context.Context, err *DLQError, records []*kgo.Record) error {
		recordsSend := make([]*kgo.Record, 0, len(records))

		for i, r := range records {
			errOrg := err.Err
			if len(err.Indexes) > 0 {
				errIndex, ok := err.Indexes[i]
				if !ok {
					continue
				}

				if errIndex != nil {
					errOrg = errIndex
				}
			}

			recordsSend = append(recordsSend, &kgo.Record{
				Topic: topic,
				Key:   r.Key,
				Value: r.Value,
				Headers: append(
					r.Headers,
					kgo.RecordHeader{Key: "process", Value: clientID},
					kgo.RecordHeader{Key: "error", Value: []byte(errOrg.Error())},
					kgo.RecordHeader{Key: "offset", Value: []byte(strconv.FormatInt(r.Offset, 10))},
					kgo.RecordHeader{Key: "partition", Value: []byte(strconv.FormatInt(int64(r.Partition), 10))},
					kgo.RecordHeader{Key: "topic", Value: []byte(r.Topic)},
					kgo.RecordHeader{Key: "timestamp", Value: []byte(r.Timestamp.Format(time.RFC3339))},
				),
			})
		}

		return fn(ctx, recordsSend)
	}
}
