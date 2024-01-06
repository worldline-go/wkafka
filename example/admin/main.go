package admin

import (
	"context"
	"sync"

	"github.com/worldline-go/wkafka"
)

var (
	kafkaConfig = wkafka.Config{
		Brokers: []string{"localhost:9092"},
	}
)

func RunExampleTopic(ctx context.Context, _ *sync.WaitGroup) error {
	client, err := wkafka.New(ctx, kafkaConfig)
	if err != nil {
		return err
	}
	defer client.Close()

	admClient := client.Admin()

	// resp, err := admClient.CreateTopic(ctx, -1, -1, nil, "test-1234")
	// if err != nil {
	// 	return err
	// }

	// slog.Info("topic created",
	// 	slog.String("topic", resp.Topic),
	// 	slog.Int64("partitions", int64(resp.NumPartitions)),
	// 	slog.Int64("replicas", int64(resp.ReplicationFactor)),
	// )

	// // list topics
	// topics, err := admClient.ListTopics(ctx)
	// if err != nil {
	// 	return err
	// }

	admClient.CreatePartitions(ctx, 2, "finops_testapp_dlq")

	// slog.Info("all topics", slog.Any("topics", topics.Names()))

	return nil
}
