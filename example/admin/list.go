package admin

import (
	"context"
	"log/slog"
	"sync"

	"github.com/worldline-go/wkafka"
)

var (
	kafkaConfigList = wkafka.Config{
		Brokers: []string{"localhost:9092"},
	}
)

func RunExampleList(ctx context.Context, _ *sync.WaitGroup) error {
	client, err := wkafka.New(ctx, kafkaConfigList, wkafka.WithPingRetry(true))
	if err != nil {
		return err
	}
	defer client.Close()

	admClient := client.Admin()

	// list topics
	topics, err := admClient.ListTopics(ctx)
	if err != nil {
		return err
	}

	slog.Info("all topics", slog.Any("topics", topics.Names()))

	return nil
}
