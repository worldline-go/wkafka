package admin

import (
	"context"
	"log/slog"
	"sync"

	"github.com/worldline-go/wkafka"
)

var (
	kafkaConfigPartition = wkafka.Config{
		Brokers: []string{"localhost:9092"},
	}
)

func RunExamplePartition(ctx context.Context, _ *sync.WaitGroup) error {
	client, err := wkafka.New(ctx, kafkaConfigPartition)
	if err != nil {
		return err
	}
	defer client.Close()

	admClient := client.Admin()

	topicName := "finops_testapp_dlq"

	admClient.CreatePartitions(ctx, 2, topicName)

	slog.Info("partitions created", slog.String("topic", topicName))

	return nil
}
