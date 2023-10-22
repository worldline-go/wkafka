package wkafka

import (
	"context"

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
	// BatchCount is a number of messages processed in a single batch.
	//  - <= 1 is 1 message per batch.
	//  - Processing count could be less than BatchCount if the batch is not full.
	//  - Usable with WithConsumerBatch
	BatchCount int `cfg:"batch_count"`
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

type Processor[T any] interface {
	Process(ctx context.Context, msg T) error
}

type ProcessorPreCheck interface {
	PreCheck(ctx context.Context, r *kgo.Record) error
}

type ProcessorDecode[T any] interface {
	Decode(raw []byte) (T, error)
}

type ProcessorDecodeWithRecord[T any] interface {
	DecodeWithRecord(raw []byte, r *kgo.Record) (T, error)
}

type consumer interface {
	config() ConsumeConfig
	Consume(ctx context.Context, cl *kgo.Client) error
}

func skip(cfg *ConsumeConfig, r *kgo.Record) bool {
	if cfg.Skip == nil {
		return false
	}

	if _, ok := cfg.Skip[r.Topic]; !ok {
		return false
	}

	if _, ok := cfg.Skip[r.Topic][r.Partition]; !ok {
		return false
	}

	offsets := cfg.Skip[r.Topic][r.Partition]

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
