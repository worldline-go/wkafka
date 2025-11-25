package demo

import (
	"context"
	"log/slog"

	"github.com/worldline-go/wkafka"
)

var KafkaConfigConsumerConcurrentBatch = wkafka.ConsumerConfig{
	Topics:     []string{"my-topic"},
	GroupID:    "my-group",
	BatchCount: 100,
	Concurrent: wkafka.ConcurrentConfig{
		Enabled: true,
		Process: 100,
		RunSize: 10000,
		Type:    wkafka.GroupTypeMixStr,
	},
}

// EXAMPLE=demo_consume_concurrent_batch
func RunConsumeConcurrentBatch(ctx context.Context) error {
	client, err := wkafka.New(ctx, KafkaConfig, wkafka.WithConsumer(KafkaConfigConsumerConcurrentBatch), wkafka.WithLogger(slog.Default()))
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Consume(ctx, wkafka.WithCallbackBatch(ConsumeCallbackConcurrentBatch))
}

func ConsumeCallbackConcurrentBatch(ctx context.Context, data []*Event) error {
	// slog.Info("message consumed", "message", data.Message)

	return nil
}
