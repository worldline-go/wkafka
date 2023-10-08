package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/worldline-go/initializer"
	"github.com/worldline-go/wkafka"
)

var (
	kafkaConfig = wkafka.Config{
		Brokers: []string{"localhost:9092"},
	}
	consumeConfig = wkafka.ConsumeConfig{
		Topics:  []string{"test"},
		GroupID: "test",
	}
)

func callBack(ctx context.Context, msg map[string]interface{}) error {
	record := wkafka.ContextRecord(ctx)

	slog.Info("callback", slog.Any("msg", msg), slog.String("topic", record.Topic), slog.String("key", string(record.Key)))

	return nil
}

func decode(data []byte) (map[string]interface{}, error) {
	if !json.Valid(data) {
		return nil, wkafka.ErrSkip
	}

	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}

	return msg, nil
}

func main() {
	initializer.Init(run)
}

func run(ctx context.Context, _ *sync.WaitGroup) error {
	client, err := wkafka.NewClient(kafkaConfig, wkafka.WithConsumer(consumeConfig, callBack, decode, nil))
	if err != nil {
		return err
	}

	defer client.Close()

	if err := client.Consume(ctx); err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	return nil
}
