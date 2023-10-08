package wkafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type options struct {
	Consumer          consumer
	ClientID          string
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
	callback func(ctx context.Context, msg T) error,
	decode func([]byte) (T, error),
	preCheck func(context.Context, *Record) error,
) Option {
	return func(o *options) {
		decode := decode
		if decode == nil {
			decoder := codecJSON[T]{}
			decode = decoder.Decode
		}

		o.Consumer = consume[T]{
			Callback: callback,
			Cfg:      cfg,
			Decode:   decode,
			PreCheck: preCheck,
		}
	}
}
