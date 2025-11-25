package demo

import (
	"context"
	"log/slog"

	"github.com/worldline-go/wkafka"
)

var KafkaConfigConsumerBatch = wkafka.ConsumerConfig{
	Topics:     []string{"my-topic"},
	GroupID:    "my-group",
	BatchCount: 500, // default 100
}

// EXAMPLE=demo_consume_batch
func RunConsumeBatch(ctx context.Context) error {
	client, err := wkafka.New(ctx, KafkaConfig, wkafka.WithConsumer(KafkaConfigConsumerBatch), wkafka.WithLogger(slog.Default()))
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Consume(ctx, wkafka.WithCallbackBatch(ConsumeCallbackBatch))
}

func ConsumeCallbackBatch(ctx context.Context, data []*Event) error {
	// slog.Info("message consumed", "message", data.Message)

	return nil
}
