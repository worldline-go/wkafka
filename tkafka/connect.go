package tkafka

import (
	"context"
	"os"
	"strings"

	"github.com/worldline-go/wkafka"
)

var (
	DefaultBrokerAddress = []string{"127.0.0.1:9092"}
)

func BrokerAddress() []string {
	brokers := DefaultBrokerAddress

	add := os.Getenv("KAFKA_BROKER")
	if add != "" {
		brokers = strings.Fields(strings.ReplaceAll(add, ",", " "))
	}

	return brokers
}

// Config returns the default configuration for Kafka for testing.
func Config() wkafka.Config {
	return wkafka.Config{
		Brokers: BrokerAddress(),
	}
}

func TestClient(opts ...OptionTestClient) (*wkafka.Client, error) {
	o := optionTestClient{
		Config:     Config(),
		Ctx:        context.Background(),
		WkafkaOpts: []wkafka.Option{wkafka.WithPingRetry(true)},
	}

	o.apply(opts...)

	if o.Ctx == nil {
		o.Ctx = context.Background()
	}

	return wkafka.New(o.Ctx, o.Config, o.WkafkaOpts...)
}

type optionTestClient struct {
	Config wkafka.Config
	Ctx    context.Context

	WkafkaOpts []wkafka.Option
}

func (o *optionTestClient) apply(opts ...OptionTestClient) {
	for _, opt := range opts {
		opt(o)
	}
}

type OptionTestClient func(*optionTestClient)

func WithContext(ctx context.Context) OptionTestClient {
	return func(o *optionTestClient) {
		o.Ctx = ctx
	}
}

func WithWafkaOptions(opts ...wkafka.Option) OptionTestClient {
	return func(o *optionTestClient) {
		o.WkafkaOpts = append(o.WkafkaOpts, opts...)
	}
}

func WithConfig(cfg wkafka.Config) OptionTestClient {
	return func(o *optionTestClient) {
		o.Config = cfg
	}
}
