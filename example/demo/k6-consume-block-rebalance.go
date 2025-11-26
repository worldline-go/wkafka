package demo

import (
	"context"
	"log/slog"
	"time"

	"github.com/worldline-go/wkafka"
)

var KafkaConfigConsumerSingleBlock = wkafka.ConsumerConfig{
	Topics:                []string{"my-topic"},
	GroupID:               "my-group",
	BlockRebalance:        wkafka.ToPtr(true),
	BlockRebalanceTimeout: time.Duration(60 * time.Second), // default 60s
}

// EXAMPLE=demo_consume_single_block
func RunConsumeSingleBlock(ctx context.Context) error {
	client, err := wkafka.New(
		ctx,
		KafkaConfig,
		wkafka.WithConsumer(KafkaConfigConsumerSingleBlock),
		wkafka.WithClientInfo("demo_consume_single_block", "v0.1.0"),
		wkafka.WithLogger(slog.Default()),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Consume(ctx, wkafka.WithCallback(ConsumeCallbackBlock))
}

func ConsumeCallbackBlock(ctx context.Context, data *Event) error {
	Sleep(ctx, 1)
	// slog.Info("message consumed", "message", data.Message)

	return nil
}
