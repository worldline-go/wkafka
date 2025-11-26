package demo

import (
	"context"
	"log/slog"

	"github.com/worldline-go/wkafka"
)

var KafkaConfigConsumerConcurrentSingle = wkafka.ConsumerConfig{
	Topics:  []string{"my-topic"},
	GroupID: "my-group",
	Concurrent: wkafka.ConcurrentConfig{
		Enabled: true,
		Process: 100,
		RunSize: 500, // default 100
		Type:    wkafka.GroupTypeMixStr,
	},
}

// EXAMPLE=demo_consume_concurrent_single
func RunConsumeConcurrentSingle(ctx context.Context) error {
	client, err := wkafka.New(
		ctx,
		KafkaConfig,
		wkafka.WithConsumer(KafkaConfigConsumerConcurrentSingle),
		wkafka.WithClientInfo("demo_consume_concurrent_single", "v0.1.0"),
		wkafka.WithLogger(slog.Default()),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Consume(ctx, wkafka.WithCallback(ConsumeCallbackConcurrentSingle))
}

func ConsumeCallbackConcurrentSingle(ctx context.Context, data *Event) error {
	Sleep(ctx, 1)
	// slog.Info("message consumed", "message", data.Message)

	return nil
}
