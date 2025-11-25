package demo

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/worldline-go/wkafka"
)

var KafkaConfigConsumerSingleError = wkafka.ConsumerConfig{
	Topics:  []string{"my-topic"},
	GroupID: "my-group",
	// Skip: map[string]map[int32]wkafka.OffsetConfig{
	// 	"my-topic": {
	// 		0: {
	// 			Offsets: []int64{5, 8},
	// 		},
	// 	},
	// },
}

// EXAMPLE=demo_consume_single_error
func RunConsumeSingleError(ctx context.Context) error {
	client, err := wkafka.New(ctx, KafkaConfig, wkafka.WithConsumer(KafkaConfigConsumerSingleError), wkafka.WithLogger(slog.Default()))
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Consume(ctx, wkafka.WithCallback(ConsumeCallbackError))
}

func ConsumeCallbackError(ctx context.Context, data *Event) error {
	if strings.Contains(data.Message, "error") {
		return fmt.Errorf("error appears in message")
	}

	return nil
}
