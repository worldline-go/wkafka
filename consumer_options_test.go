package wkafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestWithDecodeWhenDLQConsumerDisabled(t *testing.T) {
	client := &Client{
		consumerConfig:   &ConsumerConfig{DLQ: DLQConfig{ConsumerDisabled: true}},
		consumerGroup:    &group{},
		partitionHandler: &partitionHandler{},
		logger:           LogNoop{},
	}
	o := optionConsumer{Client: client, ConsumerConfig: client.consumerConfig}

	require.NoError(t, WithCallback(func(context.Context, string) error { return nil })(&o))
	require.Nil(t, o.ConsumerDLQ)
	require.NotPanics(t, func() {
		require.NoError(t, WithDecode(func([]byte, *kgo.Record) (string, error) { return "decoded", nil })(&o))
	})
}
