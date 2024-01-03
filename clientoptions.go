package wkafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultBatchCount is default batch count for batch consumer, if not set.
var DefaultBatchCount = 100

type options struct {
	ConsumerEnabled bool
	ConsumerConfig  ConsumerConfig
	// Consumer          consumer
	ClientID          string
	InfoVersion       string
	KGOOptions        []kgo.Opt
	AutoTopicCreation bool
}

type Option func(*options)

// WithClientID to set client_id in kafka server.
// Default is using DefaultClientID variable.
//
// No need to set most of time.
func WithClientID(clientID string) Option {
	return func(o *options) {
		o.ClientID = clientID
	}
}

// WithClientInfo to set client_id in kafka server.
// Not usable with WithClientID option.
//   - appname:version@hostname
func WithClientInfo(name, version string) Option {
	return func(o *options) {
		o.ClientID = name + ":" + version + "@" + idHostname
	}
}

// WithAutoTopicCreation to enable auto topic creation for producer and consumer.
//
// Default is enabled.
//
//   - Producer will fail if topic not exists and auto topic creation is disabled.
//   - Consumer will wait and not fail when is disabled.
func WithAutoTopicCreation(v bool) Option {
	return func(o *options) {
		o.AutoTopicCreation = v
	}
}

func WithKGOOptions(opts ...kgo.Opt) Option {
	return func(o *options) {
		o.KGOOptions = opts
	}
}

func WithConsumer(cfg ConsumerConfig) Option {
	return func(o *options) {
		o.ConsumerConfig = cfg
		o.ConsumerEnabled = true
	}
}

// WithConsumer sets the listener to use.
// func WithConsumer[T any](
// 	cfg ConsumeConfig,
// 	processor Processor[T],
// 	opts ...OptionSingle,
// ) Option {
// 	return func(o *options) {
// 		var decodeWithRecord func([]byte, *kgo.Record) (T, error)
// 		if v, ok := processor.(ProcessorDecodeWithRecord[T]); ok {
// 			decodeWithRecord = v.DecodeWithRecord
// 		}

// 		var decode func([]byte) (T, error)
// 		if decodeWithRecord == nil {
// 			decode = codecJSON[T]{}.Decode
// 			if v, ok := processor.(ProcessorDecode[T]); ok {
// 				decode = v.Decode
// 			}
// 		}

// 		var precheck func(context.Context, *kgo.Record) error
// 		if v, ok := processor.(ProcessorPreCheck); ok {
// 			precheck = v.PreCheck
// 		}

// 		// additional options
// 		opt := optionSingle{}
// 		opt.apply(opts...)

// 		o.Consumer = consumerSingle[T]{
// 			Process:          processor.Process,
// 			Cfg:              cfg,
// 			PreCheck:         precheck,
// 			DecodeWithRecord: decodeWithRecord,
// 			Decode:           decode,
// 			Option:           opt,
// 		}
// 	}
// }

// WithConsumer sets the listener to use.
// func WithConsumerBatch[T any](
// 	cfg ConsumerConfig,
// 	processor Processor[[]T],
// 	opts ...OptionBatch,
// ) Option {
// 	return func(o *options) {
// 		var decodeWithRecord func([]byte, *kgo.Record) (T, error)
// 		if v, ok := processor.(ProcessorDecodeWithRecord[T]); ok {
// 			decodeWithRecord = v.DecodeWithRecord
// 		}

// 		var decode func([]byte) (T, error)
// 		if decodeWithRecord == nil {
// 			decode = codecJSON[T]{}.Decode
// 			if v, ok := processor.(ProcessorDecode[T]); ok {
// 				decode = v.Decode
// 			}
// 		}

// 		var precheck func(context.Context, *kgo.Record) error
// 		if v, ok := processor.(ProcessorPreCheck); ok {
// 			precheck = v.PreCheck
// 		}

// 		if cfg.BatchCount <= 0 {
// 			cfg.BatchCount = DefaultBatchCount
// 		}

// 		// additional options
// 		opt := optionBatch{}
// 		opt.apply(opts...)

// 		o.Consumer = consumerBatch[T]{
// 			Process:          processor.Process,
// 			Cfg:              cfg,
// 			PreCheck:         precheck,
// 			DecodeWithRecord: decodeWithRecord,
// 			Decode:           decode,
// 			Option:           opt,
// 		}
// 	}
// }
