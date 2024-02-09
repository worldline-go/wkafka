package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/rs/zerolog/log"
	"github.com/worldline-go/wkafka"
)

var (
	kafkaConfigBatchErr = wkafka.Config{
		Brokers: []string{"localhost:9092"},
		Consumer: wkafka.ConsumerPreConfig{
			FormatDLQTopic: "finops_{{.AppName}}_dlq",
		},
	}
	consumeConfigBatchErr = wkafka.ConsumerConfig{
		Topics:     []string{"test"},
		GroupID:    "test_batch",
		BatchCount: 10,
	}
)

type DataBatchErr struct {
	Test       int  `json:"test"`
	IsErr      bool `json:"is_err"`
	IsErrFatal bool `json:"is_err_fatal"`
}

func ProcessBatchErr(ctx context.Context, msg []DataBatchErr) error {
	slog.Info("batch process", slog.Int("count", len(msg)))

	// prepare DLQ error
	dqlErr := wkafka.DLQError{
		Indexes: make(map[int]error, len(msg)),
	}

	records := wkafka.CtxRecordBatch(ctx)

	for i, m := range msg {
		if m.IsErr {
			// if DLQ process, return the error
			if wkafka.CtxIsDLQProcess(ctx) {
				return fmt.Errorf("test error in DLQ %d", m.Test)
			}

			// if dlq is not enabled, return the error
			if !wkafka.CtxIsDLQEnabled(ctx) {
				// batch processing error
				log.Error().
					Msgf("message err, topic:%q partition:%d offset:%d",
						records[i].Topic, records[i].Partition, records[i].Offset)

				return fmt.Errorf("dlq disabled: %d", m.Test)
			}

			dqlErr.Indexes[i] = fmt.Errorf("test error %d", m.Test)
		}

		slog.Info("callback", slog.Any("test", m.Test), slog.Bool("is_err", m.IsErr), slog.Bool("is_err_fatal", m.IsErrFatal))
	}

	if len(dqlErr.Indexes) > 0 {
		return &dqlErr
	}

	return nil
}

func RunExampleBatchErr(ctx context.Context, _ *sync.WaitGroup) error {
	client, err := wkafka.New(
		ctx, kafkaConfigBatchErr,
		wkafka.WithConsumer(consumeConfigBatchErr),
		wkafka.WithClientInfo("testappbatch", "v0.1.0"),
	)
	if err != nil {
		return err
	}

	defer client.Close()

	if err := client.Consume(ctx,
		wkafka.WithCallbackBatch(ProcessBatchErr),
	); err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	return nil
}
