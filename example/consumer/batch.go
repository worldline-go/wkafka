package consumer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/worldline-go/wkafka"
)

var (
	kafkaConfigBatch = wkafka.Config{
		Brokers: []string{"localhost:9092"},
		Consumer: wkafka.ConsumerPreConfig{
			FormatDLQTopic: "finops_{{.AppName}}_dlq",
		},
	}
	consumeConfigBatch = wkafka.ConsumerConfig{
		Topics:     []string{"test"},
		GroupID:    "test_batch",
		BatchCount: 10,
	}
)

type DataBatch struct {
	Test       int  `json:"test"`
	IsErr      bool `json:"is_err"`
	IsErrFatal bool `json:"is_err_fatal"`
}

func ProcessBatch(_ context.Context, msg []DataBatch) error {
	slog.Info("batch process", slog.Int("count", len(msg)))
	var anyError bool
	var anyErrorFatal bool
	for _, m := range msg {
		if !anyError && m.IsErr {
			anyError = true
		}

		if !anyErrorFatal && m.IsErrFatal {
			anyErrorFatal = true
		}

		slog.Info("callback", slog.Any("test", m.Test), slog.Bool("is_err", m.IsErr), slog.Bool("is_err_fatal", m.IsErrFatal))
	}

	if anyErrorFatal {
		return fmt.Errorf("test fatal error")
	}

	if anyError {
		return wkafka.WrapErrDLQ(fmt.Errorf("test error"))
	}

	return nil
}

func RunExampleBatch(ctx context.Context) error {
	client, err := wkafka.New(
		ctx, kafkaConfigBatch,
		wkafka.WithConsumer(consumeConfigBatch),
		wkafka.WithClientInfo("testappbatch", "v0.1.0"),
	)
	if err != nil {
		return err
	}

	defer client.Close()

	if err := client.Consume(ctx,
		wkafka.WithCallbackBatch(ProcessBatch),
	); err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	return nil
}
