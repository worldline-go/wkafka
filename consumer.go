package wkafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type SkipMap = map[string]map[int32]OffsetConfig

type ConsumerConfig struct {
	// Topics is a list of kafka topics to consume.
	// Required at least one topic, topic name if not exist will be created or consumer waits for topic creation.
	Topics []string `cfg:"topics" json:"topics"`
	// GroupID is the kafka consumer group ID, unique per application.
	// All created with the same ID will not be able to see messages that another consumer
	// is consuming. If a message is committed and that consumer fails for some reason,
	// another consumer with the same group ID will pick up where the other left off.
	// Required.
	GroupID string `cfg:"group_id" json:"group_id"`
	// StartOffset is used when there is no committed offset for GroupID.
	//
	// Available options:
	//      0 : Start consuming from the earliest offset.
	//     -1 : Start consuming from the latest offset.
	//  0 < n : Start consuming from the offset n.
	StartOffset int64 `cfg:"start_offset" json:"start_offset"`
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
	Skip map[string]map[int32]OffsetConfig `cfg:"skip" json:"skip"`
	// MaxPollRecords is the maximum number of records returned in a single call to poll.
	//  - Default is max.poll.records in the broker configuration, usually 500.
	//  - Fetching messages from broker, this is not related with batch processing!
	MaxPollRecords int `cfg:"max_poll_records" json:"max_poll_records"`
	// BatchCount is a number of messages processed in a single batch.
	//  - Processing count could be less than BatchCount if the batch is not full.
	//  - Usable with WithConsumerBatch
	//  - Default is 100.
	BatchCount int `cfg:"batch_count" json:"batch_count"`
	// DLQ is a dead letter queue configuration.
	DLQ DLQConfig `cfg:"dlq" json:"dlq"`
}

type DLQConfig struct {
	// Disabled is a flag to disable DLQ.
	//  - Default is false.
	//  - If topic is not set, it will be generated from format_dlq_topic.
	//  - If topic and format_dlq_topic is not set, dlq will be disabled!
	Disabled bool `cfg:"disabled" json:"disabled"`
	// RetryInterval is a time interval to retry again of DLQ messages.
	// - Default is 10 seconds.
	RetryInterval time.Duration `cfg:"retry_interval" json:"retry_interval"`
	// RetryMaxInterval is a maximum time interval to retry again of DLQ messages.
	// - Default is 15 minutes.
	RetryMaxInterval time.Duration `cfg:"retry_max_interval" json:"retry_max_interval"`
	// StartOffset is used when there is no committed offset for GroupID.
	//
	// Available options:
	//      0 : Start consuming from the earliest offset.
	//     -1 : Start consuming from the latest offset.
	//  0 < n : Start consuming from the offset n.
	StartOffset int64 `cfg:"start_offset" json:"start_offset"`
	// Skip are optional message offsets to be skipped.
	//
	// The format is a map of partition directyl and offsets.
	//  0: // partition number
	//    offsets: // list of offsets to skip
	//      - 31
	//      - 90
	//    before: 20 // skip all offsets before or equal to this offset
	Skip map[int32]OffsetConfig `cfg:"skip" json:"skip"`
	// Topic is a topic name to send messages that failed to process also could be used for DLQ.
	Topic string `cfg:"topic" json:"topic"`
	// TopicExtra is extra a list of kafka topics to just consume from DLQ.
	TopicsExtra []string `cfg:"topics_extra" json:"topics_extra"`
}

type OffsetConfig struct {
	// Offsets is a list of offsets numbers in that partition to skip.
	Offsets []int64 `cfg:"offsets" json:"offsets"`
	// Before skips all offsets before or equal to this offset.
	// In other words, before is 10, the listener skips all offsets that
	// are <= 10, so offsets 1, 2, 3, 4, 5, 6, 7, 8, 9 and 10 will be skipped
	// but not 11, 12, 13, ...
	Before *int64 `cfg:"before" json:"before"`
}

type customer[T any] struct {
	Cfg    *ConsumerConfig
	Decode func(raw []byte, r *kgo.Record) (T, error)
	// PreCheck is a function that is called before the callback and decode.
	PreCheck   func(ctx context.Context, r *kgo.Record) error
	ProduceDLQ func(ctx context.Context, err *DLQError, records []*kgo.Record) error
	Skip       func(cfg *ConsumerConfig, r *kgo.Record) bool
	Logger     Logger
	Meter      Meter
}

type consumer interface {
	Consume(ctx context.Context, cl *kgo.Client) error
	setPreCheck(fn func(ctx context.Context, r *kgo.Record) error)
}

type optionConsumer struct {
	Client         *Client
	Consumer       consumer
	ConsumerDLQ    consumer
	ConsumerConfig *ConsumerConfig
	Meter          Meter
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

// dlqProcessBatch to get one message and convert to batch message after that process.
func dlqProcessBatch[T any](fn func(ctx context.Context, msg []T) error) func(ctx context.Context, msg T) error {
	return func(ctx context.Context, msg T) error {
		return fn(ctx, []T{msg})
	}
}

// WithCallbackBatch to set wkafka consumer's callback function.
//   - Default is json.Unmarshal, use WithDecode option to add custom decode function.
//   - If [][]byte then default decode function will be skipped.
func WithCallbackBatch[T any](fn func(ctx context.Context, msg []T) error) CallBackFunc {
	return func(o *optionConsumer) error {
		decode, produceDLQ := getDecodeProduceDLQ[T](o)

		customer := customer[T]{
			Decode:     decode,
			ProduceDLQ: produceDLQ,
			Cfg:        o.ConsumerConfig,
			Skip:       newSkipper(&o.Client.consumerMutex),
			Logger:     o.Client.logger,
			Meter:      o.Meter,
		}

		o.Consumer = &consumerBatch[T]{
			customer:         &customer,
			Process:          fn,
			PartitionHandler: o.Client.partitionHandler,
		}

		if o.ConsumerConfig.DLQ.Disabled {
			return nil
		}

		o.ConsumerDLQ = &consumerBatch[T]{
			customer:         &customer,
			IsDLQ:            true,
			PartitionHandler: o.Client.partitionHandlerDLQ,
			DLQProcess: newDLQProcess(
				&customer,
				o.Client.partitionHandlerDLQ.IsRevokedRecord,
				o.Client.setDLQRecord,
				dlqProcessBatch(fn)),
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

		customer := customer[T]{
			Decode:     decode,
			ProduceDLQ: produceDLQ,
			Cfg:        o.ConsumerConfig,
			Skip:       newSkipper(&o.Client.consumerMutex),
			Logger:     o.Client.logger,
			Meter:      o.Meter,
		}

		o.Consumer = &consumerSingle[T]{
			customer:         &customer,
			Process:          fn,
			PartitionHandler: o.Client.partitionHandler,
		}

		if o.ConsumerConfig.DLQ.Disabled {
			return nil
		}

		o.ConsumerDLQ = &consumerSingle[T]{
			customer:         &customer,
			PartitionHandler: o.Client.partitionHandlerDLQ,
			IsDLQ:            true,
			DLQProcess: newDLQProcess(
				&customer,
				o.Client.partitionHandlerDLQ.IsRevokedRecord,
				o.Client.setDLQRecord,
				fn),
		}

		return nil
	}
}

func getDecodeProduceDLQ[T any](o *optionConsumer) (func(raw []byte, r *kgo.Record) (T, error), func(ctx context.Context, err *DLQError, records []*kgo.Record) error) {
	var decode func(raw []byte, r *kgo.Record) (T, error)

	var msg T
	switch any(msg).(type) {
	case []byte:
		decode = codecByte[T]{}.Decode
	default:
		decode = codecJSON[T]{}.Decode
	}

	var produceDLQ func(ctx context.Context, err *DLQError, records []*kgo.Record) error
	if !o.ConsumerConfig.DLQ.Disabled {
		produceDLQ = producerDLQ(o.ConsumerConfig.DLQ.Topic, o.Client.clientID, o.Client.ProduceRaw)
	}

	return decode, produceDLQ
}

// WithDecode to set wkafka consumer's decode function.
//   - Default is json.Unmarshal.
//   - Use this option after the WithCallback option.
func WithDecode[T any](fn func(raw []byte, r *kgo.Record) (T, error)) OptionConsumer {
	return func(o *optionConsumer) error {
		switch v := o.Consumer.(type) {
		case *consumerBatch[T]:
			v.Decode = fn
			//nolint:forcetypeassert // internally checked
			o.ConsumerDLQ.(*consumerBatch[T]).Decode = fn
		case *consumerSingle[T]:
			v.Decode = fn
			//nolint:forcetypeassert // internally checked
			o.ConsumerDLQ.(*consumerSingle[T]).Decode = fn
		default:
			return fmt.Errorf("WithDecode unknown data type %T", v)
		}

		return nil
	}
}

// WithPreCheck to set wkafka consumer's pre check function.
//   - Return ErrSkip will skip the message.
func WithPreCheck(fn func(ctx context.Context, r *kgo.Record) error) OptionConsumer {
	return func(o *optionConsumer) error {
		if o.Consumer != nil {
			o.Consumer.setPreCheck(fn)
		}
		if o.ConsumerDLQ != nil {
			o.ConsumerDLQ.setPreCheck(fn)
		}

		return nil
	}
}

// WithCallbackDLQ to set wkafka consumer's callback function for DLQ.
//   - Use this option if you want to process DLQ messages in different function.
func WithCallbackDLQ[T any](fn func(ctx context.Context, msg T) error) OptionConsumer {
	return func(o *optionConsumer) error {
		switch v := o.ConsumerDLQ.(type) {
		case *consumerBatch[T]:
			v.DLQProcess.processDLQ = fn
		case *consumerSingle[T]:
			v.DLQProcess.processDLQ = fn
		default:
			return fmt.Errorf("WithDecode unknown data type %T", v)
		}

		return nil
	}
}

func cloneSkip(skip SkipMap) SkipMap {
	if skip == nil {
		return nil
	}

	skipNew := make(SkipMap, len(skip))
	for topic, partitions := range skip {
		skipNew[topic] = make(map[int32]OffsetConfig, len(partitions))
		for partition, offset := range partitions {
			offsets := make([]int64, len(offset.Offsets))
			copy(offsets, offset.Offsets)

			skipNew[topic][partition] = OffsetConfig{
				Offsets: offsets,
				Before:  offset.Before,
			}
		}
	}

	return skipNew
}
