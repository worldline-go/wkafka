package wkafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type ConsumeConfig struct {
	// Topics is a list of kafka topics to consume.
	// Required at least one topic. Default is "default-topic" for local development.
	Topics []string `cfg:"topics" default:"default-topic"`
	// GroupID is the kafka consumer group ID, unique per application.
	// All created with the same ID will not be able to see messages that another consumer
	// is consuming. If a message is committed and that consumer fails for some reason,
	// another consumer with the same group ID will pick up where the other left off.
	// Required. Default is "default-group-id" for local development.
	GroupID string `cfg:"group_id" default:"default-group-id"`
	// StartOffset is used when there is no committed offset for GroupID.
	//
	// Available options:
	//      0 : Start consuming from the earliest offset.
	//     -1 : Start consuming from the latest offset.
	//  0 < n : Start consuming from the offset n.
	StartOffset int64 `cfg:"start_offset"`
	// Skip are optional message offsets to be skipped and not streamed by the consumer callback.
	//
	// This is a programmatic feature and message will come and check by the library.
	//
	// The format is a map of topic to partition to offsets.
	//  mytopic: // topic name
	//    0: // partition number
	//      offsets: // list of offsets to skip
	//        - 31
	//        - 90
	//      before: 20 // skip all offsets before or equal to this offset
	Skip map[string]map[int32]Offsets `cfg:"skip"`
	// MaxPollRecords is the maximum number of records returned in a single call to poll.
	//  - Default is max.poll.records in the broker configuration, usually 500.
	//  - Fetching messages from broker, this is not related with batch processing!
	MaxPollRecords int `cfg:"max_poll_records"`
	// Concurrent to run the consumer in concurrent mode for each partition and topic.
	//  - Default is false.
	//  - Each topic could have different type of value so use with processor map.
	Concurrent bool `cfg:"concurrent"`
}

type Offsets struct {
	// Offsets is a list of offsets numbers in that partition to skip.
	Offsets []int64 `cfg:"offsets"`
	// Before skips all offsets before or equal to this offset.
	// In other words, before is 10, the listener skips all offsets that
	// are <= 10, so offsets 1, 2, 3, 4, 5, 6, 7, 8, 9 and 10 will be skipped
	// but not 11, 12, 13, ...
	Before int64 `cfg:"before"`
}

type consume[T any] struct {
	Callback func(ctx context.Context, msg T) error
	Cfg      ConsumeConfig
	Decode   func(raw []byte) (T, error)
	// PreCheck is a function that is called before the callback and decode.
	PreCheck func(ctx context.Context, r *kgo.Record) error
}

type consumer interface {
	config() ConsumeConfig
	Consume(ctx context.Context, cl *kgo.Client) error
}

func (c consume[T]) config() ConsumeConfig {
	return c.Cfg
}

func (c consume[T]) skip(r *kgo.Record) bool {
	if c.Cfg.Skip == nil {
		return false
	}

	if _, ok := c.Cfg.Skip[r.Topic]; !ok {
		return false
	}

	if _, ok := c.Cfg.Skip[r.Topic][r.Partition]; !ok {
		return false
	}

	offsets := c.Cfg.Skip[r.Topic][r.Partition]

	if offsets.Before > 0 && r.Offset <= offsets.Before {
		return true
	}

	for _, offset := range offsets.Offsets {
		if r.Offset == offset {
			return true
		}
	}

	return false
}

func (c consume[T]) Consume(ctx context.Context, cl *kgo.Client) error {
	for {
		fetch := cl.PollRecords(ctx, c.Cfg.MaxPollRecords)
		if fetch.IsClientClosed() {
			return ErrClientClosed
		}

		if err := fetch.Err(); err != nil {
			return fmt.Errorf("poll fetch err: %w", err)
		}

		for iter := fetch.RecordIter(); !iter.Done(); {
			r := iter.Next()
			if err := c.iteration(ctx, r); err != nil {
				return wrapErr(r, err)
			}

			if err := cl.CommitRecords(ctx, r); err != nil {
				return wrapErr(r, fmt.Errorf("commit records failed: %w", err))
			}
		}
	}
}

func (c consume[T]) iteration(ctx context.Context, r *kgo.Record) error {
	if c.skip(r) {
		return nil
	}

	if c.PreCheck != nil {
		if err := c.PreCheck(ctx, r); err != nil {
			if errors.Is(err, ErrSkip) {
				return nil
			}

			return fmt.Errorf("pre check failed: %w", err)
		}
	}

	data, err := c.Decode(r.Value)
	if err != nil {
		if errors.Is(err, ErrSkip) {
			return nil
		}

		return fmt.Errorf("decode record failed: %w", err)
	}

	ctxCallback := context.WithValue(ctx, KeyRecord, r)
	if err := c.Callback(ctxCallback, data); err != nil {
		return fmt.Errorf("callback failed: %w", err)
	}

	return nil
}
