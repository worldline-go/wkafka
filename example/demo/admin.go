package demo

import (
	"context"
	"log/slog"

	"github.com/worldline-go/wkafka"
)

// EXAMPLE=demo_admin
func RunAdmin(ctx context.Context) error {
	client, err := wkafka.New(
		ctx,
		KafkaConfig,
		wkafka.WithLogger(slog.Default()),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	// Increase partition count to 4 for the topic
	resp, err := client.Admin().UpdatePartitions(ctx, 4, "my-topic")
	if err != nil {
		return err
	}

	// Check for any errors in the response
	for topic, topicErr := range resp {
		if topicErr.Err != nil {
			slog.Error("Failed to create partitions for topic", "topic", topic, "error", topicErr.Err)
			return topicErr.Err
		}

		slog.Info("Successfully increased partitions", "topic", topic, "partitions", 5)
	}

	return nil
}
