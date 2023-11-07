package tkafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/worldline-go/wkafka"
)

type Generate struct {
	kadm *kadm.Client

	Topics []string
}

type Topic struct {
	Name string

	// Partitions defaults to -1, which means the broker default.
	Partitions int32
	// ReplicationFactor defaults to -1, which means the broker default.
	ReplicationFactor int16
}

func NewGenerate(c *wkafka.Client) *Generate {
	return &Generate{
		kadm: c.Admin(),
	}
}

func (g *Generate) Cleanup() (kadm.DeleteTopicResponses, error) {
	return g.kadm.DeleteTopics(context.Background(), g.Topics...)
}

func (g *Generate) CreateTopics(ctx context.Context, topics ...Topic) ([]kadm.CreateTopicResponse, error) {
	responses := make([]kadm.CreateTopicResponse, 0, len(topics))
	for _, t := range topics {
		partitions := t.Partitions
		if partitions == 0 {
			partitions = -1
		}

		replicationFactor := t.ReplicationFactor
		if replicationFactor == 0 {
			replicationFactor = -1
		}

		response, err := g.kadm.CreateTopic(ctx, partitions, replicationFactor, nil, t.Name)
		g.Topics = append(g.Topics, t.Name)
		if err != nil {
			return nil, err
		}

		responses = append(responses, response)
	}

	return responses, nil
}
