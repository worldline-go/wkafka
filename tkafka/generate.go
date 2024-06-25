package tkafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/worldline-go/wkafka"
)

type Generate struct {
	kadm *kadm.Client

	Topics map[string]struct{}
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
		kadm:   c.Admin(),
		Topics: make(map[string]struct{}),
	}
}

func (g *Generate) Cleanup() (kadm.DeleteTopicResponses, error) {
	return g.kadm.DeleteTopics(context.Background(), g.getTopics()...)
}

func (g *Generate) getTopics() []string {
	topics := make([]string, 0, len(g.Topics))
	for t := range g.Topics {
		topics = append(topics, t)
	}

	return topics
}

func (g *Generate) DeleteTopics(ctx context.Context, topics ...string) (kadm.DeleteTopicResponses, error) {
	// remove the topics from the list of topics
	for _, t := range topics {
		delete(g.Topics, t)
	}

	return g.kadm.DeleteTopics(ctx, topics...)
}

func (g *Generate) DeleteGroups(ctx context.Context, groups ...string) (kadm.DeleteGroupResponses, error) {
	return g.kadm.DeleteGroups(ctx, groups...)
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
		if err != nil {
			return nil, err
		}

		g.Topics[t.Name] = struct{}{}

		responses = append(responses, response)
	}

	return responses, nil
}
