package wkafka

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func testBatchConsumer(process func(ctx context.Context, msg []*kgo.Record) error) *consumerBatch[*kgo.Record] {
	return &consumerBatch[*kgo.Record]{
		customer: &customer[*kgo.Record]{
			Cfg:    &ConsumerConfig{Concurrent: ConcurrentConfig{Enabled: true, Process: 2}},
			Skip:   func(*ConsumerConfig, *kgo.Record) bool { return false },
			Decode: func(_ []byte, r *kgo.Record) (*kgo.Record, error) { return r, nil },
			Logger: LogNoop{},
		},
		Group:            newGroupMix(2, 2),
		PartitionHandler: &partitionHandler{logger: LogNoop{}},
		Process:          process,
	}
}

func TestConsumerBatchCancellationAndResidue(t *testing.T) {
	t.Run("parent cancellation does not mark", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		records := []*kgo.Record{{Topic: "a", Offset: 0}, {Topic: "a", Offset: 1}}
		cb := testBatchConsumer(func(context.Context, []*kgo.Record) error {
			cancel()
			return nil
		})
		cl := &recordingClient{}
		require.ErrorIs(t, cb.batchIterationConcurrent(ctx, cl, testSingleFetch(records)), context.Canceled)
		require.Empty(t, cl.marked, "records processed with a cancelled context must not be marked")
	})

	t.Run("stale group records do not leak into the next call", func(t *testing.T) {
		want := errors.New("processing failed")
		first := []*kgo.Record{{Topic: "a", Offset: 0}, {Topic: "a", Offset: 1}}
		second := []*kgo.Record{{Topic: "a", Offset: 2}, {Topic: "a", Offset: 3}}
		calls := 0
		cb := testBatchConsumer(func(_ context.Context, batch []*kgo.Record) error {
			calls++
			if calls == 1 {
				return want
			}
			require.Equal(t, second, batch, "stale records from the failed call must not be re-batched")
			return nil
		})
		cl := &recordingClient{}
		require.ErrorIs(t, cb.batchIterationConcurrent(t.Context(), cl, testSingleFetch(first)), want)
		require.Empty(t, cl.marked, "failed run must not be marked")
		require.NoError(t, cb.batchIterationConcurrent(t.Context(), cl, testSingleFetch(second)))
		require.Equal(t, [][]*kgo.Record{second}, cl.marked)
	})
}
