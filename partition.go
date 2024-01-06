package wkafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

func partitionLost(c *Client) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(ctx context.Context, cl *kgo.Client, partitions map[string][]int32) {
		if len(partitions) == 0 {
			return
		}

		c.logger.Info("partition lost", "partitions", partitions)
	}
}

func partitionRevoked(c *Client) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(ctx context.Context, cl *kgo.Client, partitions map[string][]int32) {
		if len(partitions) == 0 {
			return
		}

		c.logger.Info("partition revoked", "partitions", partitions)
	}
}
