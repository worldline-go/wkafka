package tkafka

import (
	"context"
	"errors"

	"github.com/worldline-go/wkafka"
)

type ReadConfig struct {
	Name string

	KafkaConfig   wkafka.Config
	KafkaConsumer wkafka.ConsumerConfig
}

func (c *ReadConfig) sanitize() {
	if c.Name == "" {
		c.Name = "tkafka"
	}

	if c.KafkaConfig.Brokers == nil {
		c.KafkaConfig.Brokers = BrokerAddress()
	}
}

// Consume for testing creating client and start consuming messages to your function.
//   - Cancel the context to stop this function.
func Consume[T any](ctx context.Context, cfg ReadConfig, fn func(ctx context.Context, data T) error) error {
	if cfg.Name == "" {
		cfg.Name = "tkafka"
	}

	cfg.sanitize()

	// create a new client with read config
	kafkaClient, err := wkafka.New(ctx,
		cfg.KafkaConfig,
		wkafka.WithConsumer(cfg.KafkaConsumer),
		wkafka.WithAppName(cfg.Name),
	)
	if err != nil {
		return err
	}

	defer kafkaClient.Close()

	if err := kafkaClient.Consume(ctx, wkafka.WithCallback(fn)); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}

		return err
	}

	// consume messages from topic

	return nil
}
