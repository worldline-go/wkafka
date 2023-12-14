package wkafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type options struct {
	Consumer          consumer
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

// WithConsumer sets the listener to use.
func WithConsumer[T any](
	cfg ConsumeConfig,
	processor Processor[T],
) Option {
	return func(o *options) {
		var decodeWithRecord func([]byte, *kgo.Record) (T, error)
		if v, ok := processor.(ProcessorDecodeWithRecord[T]); ok {
			decodeWithRecord = v.DecodeWithRecord
		}

		var decode func([]byte) (T, error)
		if decodeWithRecord == nil {
			decode = codecJSON[T]{}.Decode
			if v, ok := processor.(ProcessorDecode[T]); ok {
				decode = v.Decode
			}
		}

		var precheck func(context.Context, *kgo.Record) error
		if v, ok := processor.(ProcessorPreCheck); ok {
			precheck = v.PreCheck
		}

		o.Consumer = consumerSingle[T]{
			Process:          processor.Process,
			Cfg:              cfg,
			PreCheck:         precheck,
			DecodeWithRecord: decodeWithRecord,
			Decode:           decode,
		}
	}
}

// WithConsumer sets the listener to use.
func WithConsumerBatch[T any](
	cfg ConsumeConfig,
	processor Processor[[]T],
) Option {
	return func(o *options) {
		var decodeWithRecord func([]byte, *kgo.Record) (T, error)
		if v, ok := processor.(ProcessorDecodeWithRecord[T]); ok {
			decodeWithRecord = v.DecodeWithRecord
		}

		var decode func([]byte) (T, error)
		if decodeWithRecord == nil {
			decode = codecJSON[T]{}.Decode
			if v, ok := processor.(ProcessorDecode[T]); ok {
				decode = v.Decode
			}
		}

		var precheck func(context.Context, *kgo.Record) error
		if v, ok := processor.(ProcessorPreCheck); ok {
			precheck = v.PreCheck
		}

		if cfg.BatchCount <= 0 {
			cfg.BatchCount = 1
		}

		o.Consumer = consumerBatch[T]{
			Process:          processor.Process,
			Cfg:              cfg,
			PreCheck:         precheck,
			DecodeWithRecord: decodeWithRecord,
			Decode:           decode,
		}
	}
}
