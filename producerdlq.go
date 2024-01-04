package wkafka

import (
	"context"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// producerDLQ to push failed records to dead letter queue.
//   - errs must be the same length as records or 1.
func producerDLQ(topic string, fn func(ctx context.Context, records []*kgo.Record) error) func(ctx context.Context, errs []error, records []*kgo.Record) error {
	return func(ctx context.Context, errs []error, records []*kgo.Record) error {
		recordsModified := make([]*kgo.Record, len(records))
		for i, r := range recordsModified {
			var err error
			if len(errs) == 1 {
				err = errs[0]
			} else {
				err = errs[i]
			}

			recordsModified[i] = &kgo.Record{
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
			}
		}

		return fn(ctx, records)
	}
}
