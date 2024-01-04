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
