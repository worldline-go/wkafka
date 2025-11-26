package demo

import (
	"context"
	"log/slog"

	"github.com/worldline-go/wkafka"
)

var KafkaConfigConsumerBatch = wkafka.ConsumerConfig{
	Topics:     []string{"my-topic"},
	GroupID:    "my-group",
	BatchCount: 200, // default 100
}

// EXAMPLE=demo_consume_batch
func RunConsumeBatch(ctx context.Context) error {
	client, err := wkafka.New(
		ctx,
		KafkaConfig,
		wkafka.WithConsumer(KafkaConfigConsumerBatch),
		wkafka.WithClientInfo("demo_consume_batch", "v0.1.0"),
		wkafka.WithLogger(slog.Default()),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Consume(ctx, wkafka.WithCallbackBatch(ConsumeCallbackBatch))
}

func ConsumeCallbackBatch(ctx context.Context, data []*Event) error {
	Sleep(ctx, len(data))

	// slog.Info("message consumed", "message", data.Message)

	return nil
}
