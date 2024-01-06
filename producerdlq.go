package wkafka

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// producerDLQ to push failed records to dead letter queue.
//   - err could be ErrDLQIndexed or any other error
func producerDLQ(topic string, fn func(ctx context.Context, records []*kgo.Record) error) func(ctx context.Context, err error, records []*kgo.Record) error {
	return func(ctx context.Context, err error, records []*kgo.Record) error {
		recordsSend := make([]*kgo.Record, 0, len(records))

		errDLQIndexed := &DLQIndexedError{}
		if !errors.As(err, &errDLQIndexed) {
			errDLQIndexed.Err = err
		}

		for i, r := range records {
			err := errDLQIndexed.Err
			if len(errDLQIndexed.Indexes) > 0 {
				if err := errDLQIndexed.Indexes[i]; err == nil {
					continue
				}
			} else {
				// ErrDLQ used, unwrap and show original error
				err = unwrapErr(err)
			}

			recordsSend = append(recordsSend, &kgo.Record{
				Topic: topic,
				Key:   r.Key,
				Value: r.Value,
				Headers: append(
					r.Headers,
					kgo.RecordHeader{Key: "error", Value: []byte(err.Error())},
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
