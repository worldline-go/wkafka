package wkafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type ConsumerConfig struct {
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
	// BatchCount is a number of messages processed in a single batch.
	//  - <= 1 is 1 message per batch.
	//  - Processing count could be less than BatchCount if the batch is not full.
	//  - Usable with WithConsumerBatch
	BatchCount int `cfg:"batch_count"`
	// DLQ is a dead letter queue configuration.
	DLQ DLQ `cfg:"dlq"`
}

type DLQ struct {
	// Enable is a flag to enable DLQ.
	Enable bool `cfg:"enable"`
	// Skip are optional message offsets to be skipped.
	//
	// The format is a map of partition directyl and offsets.
	//  0: // partition number
	//    offsets: // list of offsets to skip
	//      - 31
	//      - 90
	//    before: 20 // skip all offsets before or equal to this offset
	Skip map[int32]Offsets `cfg:"skip"`
	// Topic is a topic name to send messages that failed to process.
	//  - Default is empty, no DLQ.
	Topic string `cfg:"topic"`
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
	Decode(raw []byte, r *kgo.Record) (T, error)
}

type consumer interface {
	Consume(ctx context.Context, cl *kgo.Client) error
	setPreCheck(fn func(ctx context.Context, r *kgo.Record) error)
}

func skip(cfg *ConsumerConfig, r *kgo.Record) bool {
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

type optionConsumer struct {
	Client         *Client
	Consumer       consumer
	ConsumerConfig ConsumerConfig
	// Concurrent to run the consumer in concurrent mode for each partition and topic.
	//  - Default is false.
	//  - Each topic could have different type of value so use with processor map.
	Concurrent bool `cfg:"concurrent"`
}

type (
	OptionConsumer func(*optionConsumer) error
	CallBackFunc   func(*optionConsumer) error
)

func (o *optionConsumer) apply(opts ...OptionConsumer) error {
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return err
		}
	}

	return nil
}

// WithCallbackBatch to set wkafka consumer's callback function.
//   - Default is json.Unmarshal, use WithDecode option to add custom decode function.
//   - If [][]byte then default decode function will be skipped.
func WithCallbackBatch[T any](fn func(ctx context.Context, msg []T) error) CallBackFunc {
	return func(o *optionConsumer) error {
		decode, produceDLQ := getDecodeProduceDLQ[T](o)

		o.Consumer = &consumerBatch[T]{
			Process:    fn,
			Decode:     decode,
			ProduceDLQ: produceDLQ,
			Cfg:        o.ConsumerConfig,
		}

		return nil
	}
}

// WithCallback to set wkafka consumer's callback function.
//   - Default is json.Unmarshal, use WithDecode option to add custom decode function.
//   - If []byte then default decode function will be skipped.
func WithCallback[T any](fn func(ctx context.Context, msg T) error) CallBackFunc {
	return func(o *optionConsumer) error {
		decode, produceDLQ := getDecodeProduceDLQ[T](o)

		o.Consumer = &consumerSingle[T]{
			Process:    fn,
			Decode:     decode,
			ProduceDLQ: produceDLQ,
			Cfg:        o.ConsumerConfig,
		}

		return nil
	}
}

func getDecodeProduceDLQ[T any](o *optionConsumer) (func(raw []byte, r *kgo.Record) (T, error), func(ctx context.Context, records []*kgo.Record) error) {
	var decode func(raw []byte, r *kgo.Record) (T, error)

	var msg T
	switch any(msg).(type) {
	case []byte:
		decode = codecByte[T]{}.Decode
	default:
		decode = codecJSON[T]{}.Decode
	}

	var produceDLQ func(ctx context.Context, records []*kgo.Record) error
	if o.ConsumerConfig.DLQ.Enable {
		produceDLQ = o.Client.ProduceRaw
	}

	return decode, produceDLQ
}

// WithDecode to set wkafka consumer's decode function.
//   - Default is json.Unmarshal.
//   - Use this option after the WithCallback option.
func WithDecode[T any](fn func(raw []byte, r *kgo.Record) (T, error)) OptionConsumer {
	return func(o *optionConsumer) error {
		if o.Consumer == nil {
			return fmt.Errorf("consumer is nil, use WithCallback[Batch] option first")
		}

		switch v := o.Consumer.(type) {
		case *consumerBatch[T]:
			v.Decode = fn
		case *consumerSingle[T]:
			v.Decode = fn
		}

		return nil
	}
}

func WithPreCheck(fn func(ctx context.Context, r *kgo.Record) error) OptionConsumer {
	return func(o *optionConsumer) error {
		if o.Consumer == nil {
			return fmt.Errorf("consumer is nil, use WithCallback[Batch] option first")
		}

		o.Consumer.setPreCheck(fn)

		return nil
	}
}

// TODO implement concurrent mode
// func WithConcurrent() OptionConsumer {
// 	return func(o *optionConsumer) error {
// 		o.Concurrent = true

// 		return nil
// 	}
// }
