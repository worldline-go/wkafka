package demo

import (
	"context"
	"log/slog"

	"github.com/worldline-go/wkafka"
)

var KafkaConfigConsumerSingle = wkafka.ConsumerConfig{
	Topics:  []string{"my-topic"},
	GroupID: "my-group",
}

// EXAMPLE=demo_consume_single
func RunConsumeSingle(ctx context.Context) error {
	client, err := wkafka.New(
		ctx,
		KafkaConfig,
		wkafka.WithConsumer(KafkaConfigConsumerSingle),
		wkafka.WithClientInfo("demo_consume_single", "v0.1.0"),
		wkafka.WithLogger(slog.Default()),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Consume(ctx, wkafka.WithCallback(ConsumeCallback))
}

func ConsumeCallback(ctx context.Context, data *Event) error {
	Sleep(ctx, 1)

	return nil
}
