package demo

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/worldline-go/wkafka"
	"github.com/worldline-go/wkafka/plugins/handler"
)

var KafkaConfigUI = wkafka.Config{
	Brokers: []string{"localhost:9092"},
	Plugins: map[string]any{
		handler.PluginName: map[string]any{
			"enabled": true,
			"addr":    ":8080",
			"pubsub": map[string]any{
				"prefix": "wkafka_",
				"redis": map[string]any{
					"address": "localhost:6379",
				},
			},
		},
	},
}

var KafkaConfigConsumerSingleErrorDLQUI = wkafka.ConsumerConfig{
	Topics:  []string{"my-topic"},
	GroupID: "my-group",
	DLQ: wkafka.DLQConfig{
		Topic: "my-group-dlq",
	},
}

// EXAMPLE=demo_consume_single_error_dlq_ui
func RunConsumeSingleErrorDLQUI(ctx context.Context) error {
	client, err := wkafka.New(ctx, KafkaConfigUI,
		wkafka.WithConsumer(KafkaConfigConsumerSingleErrorDLQUI),
		wkafka.WithLogger(slog.Default()),
		wkafka.WithPlugin(handler.PluginWithName()),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Consume(ctx, wkafka.WithCallback(ConsumeCallbackErrorDLQUI))
}

func ConsumeCallbackErrorDLQUI(ctx context.Context, data *Event) error {
	if strings.Contains(data.Message, "error") {
		return wkafka.WrapErrDLQ(fmt.Errorf("error appears in message"))
	}

	return nil
}
