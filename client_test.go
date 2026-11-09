package wkafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestClientCallTrigger(t *testing.T) {
	c := &Client{}
	c.callTrigger(t.Context())

	called := make(chan context.Context, 1)
	c.AddTrigger(func(ctx context.Context) { called <- ctx })
	c.callTrigger(t.Context())
	select {
	case ctx := <-called:
		require.Equal(t, t.Context(), ctx)
	case <-time.After(5 * time.Second):
		t.Fatal("registered trigger was not called")
	}
}

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

func TestRegisterPlugin(t *testing.T) {
	type pluginConfig struct {
		Value string `cfg:"value"`
	}

	var calls []string
	RegisterPlugin("test-registered", func(_ context.Context, _ *Client, cfg pluginConfig) error {
		calls = append(calls, "registered:"+cfg.Value)
		return nil
	})
	RegisterPlugin("test-override", func(_ context.Context, _ *Client, _ pluginConfig) error {
		calls = append(calls, "override-registered")
		return nil
	})
	t.Cleanup(func() {
		registeredPlugins.m.Lock()
		delete(registeredPlugins.holder, "test-registered")
		delete(registeredPlugins.holder, "test-override")
		registeredPlugins.m.Unlock()
	})

	client, err := New(t.Context(), Config{
		Brokers: []string{"127.0.0.1:1"},
		Plugins: map[string]any{"test-registered": map[string]any{"value": "x"}},
	}, WithPing(false), WithPlugin("test-override", func(_ context.Context, _ *Client, _ pluginConfig) error {
		calls = append(calls, "override-option")
		return nil
	}))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.ElementsMatch(t, []string{"registered:x", "override-option"}, calls)
}
