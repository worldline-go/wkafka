package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/worldline-go/initializer"
	"github.com/worldline-go/wkafka"
)

var (
	kafkaConfig = wkafka.Config{
		Brokers: []string{"localhost:9092"},
	}
	consumeConfig = wkafka.ConsumerConfig{
		Topics:     []string{"test"},
		GroupID:    "test1",
		BatchCount: 2,
	}
)

type Data struct {
	Test int `json:"test"`

	Metadata `json:"-"`
}

type Metadata struct {
	Topic string
	Key   []byte
}

type Processor struct{}

func (Processor) Process(_ context.Context, msg []Data) error {
	slog.Info("batch process", slog.Int("count", len(msg)))
	for _, m := range msg {
		slog.Info("callback", slog.Any("test", m.Test), slog.String("topic", m.Metadata.Topic), slog.String("key", string(m.Metadata.Key)))
	}

	return nil
}

func (Processor) Decode(data []byte, r *kgo.Record) (Data, error) {
	if !json.Valid(data) {
		return Data{}, wkafka.ErrSkip
	}

	var msg Data
	if err := json.Unmarshal(data, &msg); err != nil {
		return Data{}, err
	}

	msg.Metadata.Topic = r.Topic
	msg.Metadata.Key = r.Key

	return msg, nil
}

func main() {
	initializer.Init(run)
}

func run(ctx context.Context, _ *sync.WaitGroup) error {
	p := Processor{}

	client, err := wkafka.New(
		ctx, kafkaConfig,
		wkafka.WithConsumer(consumeConfig),
		wkafka.WithClientInfo("testapp", "v0.1.0"),
	)
	if err != nil {
		return err
	}

	defer client.Close()

	if err := client.Consume(ctx,
		wkafka.WithCallbackBatch(p.Process),
		wkafka.WithDecode(p.Decode),
	); err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	return nil
}
