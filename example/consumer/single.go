package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/worldline-go/wkafka"
)

var (
	kafkaConfigSingle = wkafka.Config{
		Brokers: []string{"localhost:9092"},
		Consumer: wkafka.ConsumerPreConfig{
			FormatDLQTopic: "finops_{{.AppName}}_dlq",
		},
	}
	consumeConfigSingle = wkafka.ConsumerConfig{
		Topics:  []string{"test"},
		GroupID: "test_single",
	}
)

type DataSingle struct {
	Test       int  `json:"test"`
	IsErr      bool `json:"is_err"`
	IsErrFatal bool `json:"is_err_fatal"`
}

func ProcessSingle(_ context.Context, msg DataSingle) error {
	slog.Info("callback", slog.Any("test", msg.Test), slog.Bool("is_err", msg.IsErr))

	if msg.IsErrFatal {
		return fmt.Errorf("test fatal error %d", msg.Test)
	}

	if msg.IsErr {
		return fmt.Errorf("test error %d: %w", msg.Test, wkafka.ErrDLQ)
	}

	return nil
}

func RunExampleSingle(ctx context.Context, _ *sync.WaitGroup) error {
	client, err := wkafka.New(
		ctx, kafkaConfigSingle,
		wkafka.WithConsumer(consumeConfigSingle),
		wkafka.WithClientInfo("testapp", "v0.1.0"),
	)
	if err != nil {
		return err
	}

	defer client.Close()

	if err := client.Consume(ctx, wkafka.WithCallback(ProcessSingle)); err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	return nil
}
