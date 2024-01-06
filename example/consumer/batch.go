package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/worldline-go/wkafka"
)

var (
	kafkaConfigBatch = wkafka.Config{
		Brokers: []string{"localhost:9092"},
	}
	consumeConfigBatch = wkafka.ConsumerConfig{
		Topics:     []string{"test"},
		GroupID:    "test_batch",
		BatchCount: 5,
	}
)

type DataBatch struct {
	Test int `json:"test"`

	MetadataBatch `json:"-"`
}

type MetadataBatch struct {
	Topic string
	Key   []byte
}

func ProcessBatch(_ context.Context, msg []DataBatch) error {
	slog.Info("batch process", slog.Int("count", len(msg)))
	for _, m := range msg {
		slog.Info("callback", slog.Any("test", m.Test), slog.String("topic", m.MetadataBatch.Topic), slog.String("key", string(m.MetadataBatch.Key)))
	}

	return nil
}

func Decode(data []byte, r *kgo.Record) (DataBatch, error) {
	if !json.Valid(data) {
		return DataBatch{}, wkafka.ErrSkip
	}

	var msg DataBatch
	if err := json.Unmarshal(data, &msg); err != nil {
		return DataBatch{}, err
	}

	msg.MetadataBatch.Topic = r.Topic
	msg.MetadataBatch.Key = r.Key

	return msg, nil
}

func RunExampleBatch(ctx context.Context, _ *sync.WaitGroup) error {
	client, err := wkafka.New(
		ctx, kafkaConfigBatch,
		wkafka.WithConsumer(consumeConfigBatch),
		wkafka.WithClientInfo("testapp", "v0.1.0"),
	)
	if err != nil {
		return err
	}

	defer client.Close()

	if err := client.Consume(ctx,
		wkafka.WithCallbackBatch(ProcessBatch),
		wkafka.WithDecode(Decode),
	); err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	return nil
}
