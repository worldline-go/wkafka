package wkafka

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

type consumerFunc func(context.Context, client) error

func (f consumerFunc) Consume(ctx context.Context, cl client) error {
	return f(ctx, cl)
}

func (consumerFunc) setPreCheck(func(context.Context, *kgo.Record) error) {}

func TestClientConsumeReportsDLQTopics(t *testing.T) {
	dlqErr := errors.New("DLQ unavailable")
	c := &Client{
		Kafka:          &kgo.Client{},
		KafkaDLQ:       &kgo.Client{},
		consumerConfig: &ConsumerConfig{},
		logger:         LogNoop{},
		hook:           &hooker{},
		topics:         []string{"main-topic"},
		dlqTopics:      []string{"dlq-topic"},
	}

	err := c.Consume(t.Context(), func(*optionConsumer) error { return nil }, func(o *optionConsumer) error {
		o.Consumer = consumerFunc(func(ctx context.Context, _ client) error {
			<-ctx.Done()
			return nil
		})
		o.ConsumerDLQ = consumerFunc(func(context.Context, client) error { return dlqErr })

		return nil
	})

	require.ErrorIs(t, err, dlqErr)
	require.EqualError(t, err, "failed to consume DLQ [dlq-topic]: DLQ unavailable")
}

func TestNewCleansUpWhenPluginFails(t *testing.T) {
	pluginErr := errors.New("plugin failed")
	var pluginClient *Client
	var pluginContext context.Context

	_, err := New(t.Context(), Config{Brokers: []string{"127.0.0.1:1"}}, WithPing(false), WithPlugin("failing", func(ctx context.Context, client *Client, _ struct{}) error {
		pluginClient = client
		pluginContext = ctx
		return pluginErr
	}))

	require.ErrorIs(t, err, pluginErr)
	require.NotNil(t, pluginClient)
	produceResult := pluginClient.Kafka.ProduceSync(t.Context(), &kgo.Record{Topic: "test"})
	require.ErrorIs(t, produceResult.FirstErr(), kgo.ErrClientClosed)
	require.ErrorIs(t, pluginContext.Err(), context.Canceled)
}
