package wkafka

import (
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultBatchCount is default batch count for batch consumer, if not set.
var DefaultBatchCount = 100

func defaultBackoff() backoff.BackOff {
	return backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(2*time.Second),
		backoff.WithMaxInterval(7*time.Second),
		backoff.WithMaxElapsedTime(30*time.Second),
	)
}

type options struct {
	AppName            string
	ConsumerEnabled    bool
	ConsumerDLQEnabled bool
	ConsumerConfig     *ConsumerConfig
	// Consumer          consumer
	ClientID          string
	KGOOptions        []kgo.Opt
	KGOOptionsDLQ     []kgo.Opt
	AutoTopicCreation bool
	Logger            Logger
	Meter             Meter

	Ping        bool
	PingRetry   bool
	PingBackoff backoff.BackOff
}

func (o *options) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

type Option func(*options)

// WithClientID to set client_id in kafka server.
// Default is using DefaultClientID variable.
//
// No need to set most of time!
func WithClientID(clientID string) Option {
	return func(o *options) {
		o.ClientID = clientID
	}
}

func WithMeter(m Meter) Option {
	return func(o *options) {
		o.Meter = m
	}
}

// WithClientInfo to set client_id in kafka server.
// Not usable with WithClientID option.
//   - appname:version@hostname
func WithClientInfo(appName, version string) Option {
	return func(o *options) {
		o.ClientID = appName + ":" + version + "@" + idHostname
		o.AppName = appName
	}
}

// WithAppName to set app name in kafka server.
// Default is using idProgname variable.
//
// Use WithClientInfo instead if you want to set version and appname.
func WithAppName(appName string) Option {
	return func(o *options) {
		o.ClientID = appName + "@" + idHostname
		o.AppName = appName
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

// WithKGOOptions to set kgo options.
func WithKGOOptions(opts ...kgo.Opt) Option {
	return func(o *options) {
		o.KGOOptions = append(o.KGOOptions, opts...)
	}
}

// WithKGOOptionsDLQ to set kgo options for DLQ client.
//
// If empty than it will use same options as main client.
func WithKGOOptionsDLQ(opts ...kgo.Opt) Option {
	return func(o *options) {
		o.KGOOptionsDLQ = append(o.KGOOptionsDLQ, opts...)
	}
}

// WithConsumer configures the client to use the provided consumer config.
//   - It is shallow copied and to make safe use skip function to modify skip map.
func WithConsumer(cfg ConsumerConfig) Option {
	return func(o *options) {
		o.ConsumerConfig = &cfg
		o.ConsumerEnabled = true
	}
}

// WithLogger configures the client to use the provided logger.
//   - For zerolog logz.AdapterKV{Log: logger} can usable.
//   - Default is using zerolog's global logger.
func WithLogger(logger Logger) Option {
	return func(o *options) {
		o.Logger = logger
	}
}

// WithNoLogger to disable logger.
func WithNoLogger(v bool) Option {
	return func(o *options) {
		if v {
			o.Logger = LogNoop{}
		}
	}
}

// WithPing to ping kafka brokers on client creation.
//   - Default is enabled.
func WithPing(v bool) Option {
	return func(o *options) {
		o.Ping = v
	}
}

// WithPingRetry to retry ping kafka brokers on client creation.
func WithPingRetry(v bool) Option {
	return func(o *options) {
		o.PingRetry = v
	}
}

func WithPingBackoff(b backoff.BackOff) Option {
	return func(o *options) {
		o.PingBackoff = b
	}
}
