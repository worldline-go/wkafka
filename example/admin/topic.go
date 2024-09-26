package admin

import (
	"context"
	"log/slog"

	"github.com/worldline-go/wkafka"
)

var (
	kafkaConfigTopic = wkafka.Config{
		Brokers: []string{"localhost:9092"},
	}
)

func RunExampleTopic(ctx context.Context) error {
	client, err := wkafka.New(ctx, kafkaConfigTopic)
	if err != nil {
		return err
	}
	defer client.Close()

	admClient := client.Admin()

	resp, err := admClient.CreateTopic(ctx, -1, -1, nil, "test-1234")
	if err != nil {
		return err
	}

	slog.Info("topic created",
		slog.String("topic", resp.Topic),
		slog.Int64("partitions", int64(resp.NumPartitions)),
		slog.Int64("replicas", int64(resp.ReplicationFactor)),
	)

	return nil
}
