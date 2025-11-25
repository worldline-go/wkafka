package demo

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/worldline-go/wkafka"
)

var KafkaConfig = wkafka.Config{
	Brokers: []string{"localhost:9092"},
}

type Event struct {
	Message string `json:"message"`
}

// EXAMPLE=demo_push
func RunPush(ctx context.Context) error {
	client, err := wkafka.New(ctx, KafkaConfig, wkafka.WithLogger(slog.Default()))
	if err != nil {
		return err
	}
	defer client.Close()

	topic := "my-topic"

	// Creating a producer with generic need to have a new function and accept client as parameter.
	producer, err := wkafka.NewProducer[*Event](
		client,
		topic,
		wkafka.WithHook(func(v *Event, record *kgo.Record) error {
			record.Key = []byte(v.Message)
			return nil
		}),
	)
	if err != nil {
		return err
	}

	totalMessage := 10_000_000
	data := make([]*Event, 0, totalMessage)
	for i := range totalMessage {
		data = append(data, &Event{Message: "message-" + strconv.Itoa(i)})
	}

	if err := producer.Produce(ctx, data...); err != nil {
		return err
	}

	slog.Info("messages pushed successfully", "topic", topic, "total", totalMessage)

	return nil
}
