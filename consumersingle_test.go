package wkafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Only the consumer's polling and marking API is implemented: no offset mutation.
type recordingClient struct {
	marked [][]*kgo.Record
	poll   func(context.Context, int) kgo.Fetches
}

func (c *recordingClient) MarkCommitRecords(records ...*kgo.Record) {
	c.marked = append(c.marked, append([]*kgo.Record(nil), records...))
}

func (c *recordingClient) AllowRebalance() {}

func (c *recordingClient) PollRecords(ctx context.Context, max int) kgo.Fetches {
	return c.poll(ctx, max)
}

func testSingleConsumer(process func(context.Context, *kgo.Record) error) *consumerSingle[*kgo.Record] {
	return &consumerSingle[*kgo.Record]{
		customer: &customer[*kgo.Record]{
			Cfg:    &ConsumerConfig{Concurrent: ConcurrentConfig{Enabled: true, Process: 2}},
			Skip:   func(*ConsumerConfig, *kgo.Record) bool { return false },
			Decode: func(_ []byte, r *kgo.Record) (*kgo.Record, error) { return r, nil },
			Logger: LogNoop{},
		},
		Group:            newGroupPartition(10, 10, 1),
		PartitionHandler: &partitionHandler{logger: LogNoop{}},
		Process:          process,
	}
}

func testSingleFetch(partitions ...[]*kgo.Record) kgo.Fetches {
	var fetch kgo.Fetches
	for _, records := range partitions {
		fetch = append(fetch, kgo.Fetch{Topics: []kgo.FetchTopic{{
			Topic:      records[0].Topic,
			Partitions: []kgo.FetchPartition{{Partition: records[0].Partition, Records: records}},
		}}})
	}
	return fetch
}

func TestConsumerSingleConcurrentPartitions(t *testing.T) {
	for _, second := range []topicPartition{{topic: "a", partition: 1}, {topic: "b", partition: 0}} {
		t.Run(second.topic, func(t *testing.T) {
			partitions := [][]*kgo.Record{
				{{Topic: "a", Partition: 0, Offset: 0}, {Topic: "a", Partition: 0, Offset: 1}},
				{{Topic: second.topic, Partition: second.partition, Offset: 0}, {Topic: second.topic, Partition: second.partition, Offset: 1}},
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			started := make(chan struct{}, 2)
			release := make(chan struct{})
			var mu sync.Mutex
			order := make(map[topicPartition][]int64)
			cs := testSingleConsumer(func(ctx context.Context, r *kgo.Record) error {
				if r.Offset == 0 {
					started <- struct{}{}
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				mu.Lock()
				defer mu.Unlock()
				key := topicPartition{topic: r.Topic, partition: r.Partition}
				order[key] = append(order[key], r.Offset)
				return nil
			})
			cl := &recordingClient{}
			done := make(chan error, 1)
			go func() { done <- cs.iterationConcurrent(ctx, cl, testSingleFetch(partitions...)) }()
			// Both first records must start before either can finish; no sleeps or timing assertions.
			for range 2 {
				select {
				case <-started:
				case <-ctx.Done():
					cancel()
					<-done
					t.Fatal("partitions did not start concurrently")
				}
			}
			close(release)
			require.NoError(t, <-done)
			require.Equal(t, []int64{0, 1}, order[topicPartition{topic: "a", partition: 0}])
			require.Equal(t, []int64{0, 1}, order[second])
			require.Equal(t, [][]*kgo.Record{append(partitions[0], partitions[1]...)}, cl.marked)
		})
	}
}

func TestConsumerSingleConsumeReturnsProcessingError(t *testing.T) {
	want := errors.New("processing failed")
	records := []*kgo.Record{{Topic: "a", Offset: 0}, {Topic: "a", Offset: 1}, {Topic: "a", Offset: 2}, {Topic: "a", Offset: 3}, {Topic: "a", Offset: 4}}
	var processed []int64
	cs := testSingleConsumer(func(_ context.Context, r *kgo.Record) error {
		processed = append(processed, r.Offset)
		if r.Offset == 3 {
			return want
		}
		return nil
	})
	cs.Group = newGroupPartition(2, 2, 1)
	cl := &recordingClient{}
	polls := 0
	cl.poll = func(context.Context, int) kgo.Fetches {
		polls++
		require.Equal(t, 1, polls, "must return without retrying or polling again")
		return testSingleFetch(records)
	}
	require.ErrorIs(t, cs.Consume(t.Context(), cl), want)
	require.Equal(t, []int64{0, 1, 2, 3}, processed)
	require.Equal(t, [][]*kgo.Record{records[:2]}, cl.marked, "failed run must not be marked")
}

func TestConsumerSingleFailedRunDoesNotMarkOtherPartitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	want := errors.New("processing failed")
	otherFinished := make(chan struct{})
	cs := testSingleConsumer(func(ctx context.Context, r *kgo.Record) error {
		if r.Partition == 1 {
			close(otherFinished)
			return nil
		}
		select {
		case <-otherFinished:
			return want
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	cl := &recordingClient{}
	fetch := testSingleFetch([]*kgo.Record{{Topic: "a", Partition: 0}}, []*kgo.Record{{Topic: "a", Partition: 1}})
	require.ErrorIs(t, cs.iterationConcurrent(ctx, cl, fetch), want)
	require.Empty(t, cl.marked, "even successful partitions in the failed run must remain unmarked")
}

func TestConsumerSingleDLQRouting(t *testing.T) {
	ordinary := errors.New("ordinary failure")
	produceErr := errors.New("DLQ unavailable")
	for _, tt := range []struct {
		name       string
		processErr error
		enabled    bool
		produceErr error
		want       error
		calls      int
	}{
		{name: "wrapped DLQ", processErr: WrapErrDLQ(ordinary), enabled: true, calls: 1},
		{name: "DLQ sentinel", processErr: ErrDLQ, enabled: true, calls: 1},
		{name: "non-DLQ", processErr: ordinary, enabled: true, want: ordinary},
		{name: "DLQ disabled", processErr: ErrDLQ, want: ErrDLQ},
		{name: "DLQ production fails", processErr: ErrDLQ, enabled: true, produceErr: produceErr, want: produceErr, calls: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := &kgo.Record{Topic: "a"}
			cs := testSingleConsumer(func(context.Context, *kgo.Record) error { return tt.processErr })
			calls := 0
			if tt.enabled {
				cs.ProduceDLQ = func(_ context.Context, err *DLQError, records []*kgo.Record) error {
					calls++
					if !errors.Is(err, tt.processErr) || len(records) != 1 || records[0] != r {
						return errors.New("incorrect DLQ routing")
					}
					return tt.produceErr
				}
			}
			cl := &recordingClient{}
			err := cs.iterationConcurrent(t.Context(), cl, testSingleFetch([]*kgo.Record{r}))
			require.ErrorIs(t, err, tt.want)
			require.Equal(t, tt.calls, calls)
			if tt.want == nil {
				require.Equal(t, [][]*kgo.Record{{r}}, cl.marked)
			} else {
				require.Empty(t, cl.marked)
			}
		})
	}
}

func TestConsumerSingleCancellationAndRevocation(t *testing.T) {
	t.Run("parent cancellation does not mark", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		cs := testSingleConsumer(func(context.Context, *kgo.Record) error { cancel(); return nil })
		cs.Group = newGroupMix(1, 1)
		cl := &recordingClient{}
		require.ErrorIs(t, cs.iterationConcurrent(ctx, cl, testSingleFetch([]*kgo.Record{{Topic: "a"}})), context.Canceled)
		require.Empty(t, cl.marked)
	})
	t.Run("revoked records are filtered before processing and marking", func(t *testing.T) {
		var processed []*kgo.Record
		cs := testSingleConsumer(nil)
		cs.PartitionHandler.AddPartitionsRevoked(map[string][]int32{"a": {0}})
		cs.Process = func(_ context.Context, r *kgo.Record) error {
			processed = append(processed, r)
			cs.PartitionHandler.AddPartitionsRevoked(map[string][]int32{"a": {1}})
			return nil
		}
		r := &kgo.Record{Topic: "a", Partition: 1}
		cl := &recordingClient{}
		require.NoError(t, cs.iterationConcurrent(t.Context(), cl, testSingleFetch([]*kgo.Record{{Topic: "a"}}, []*kgo.Record{r})))
		require.Equal(t, []*kgo.Record{r}, processed)
		require.Empty(t, cl.marked)
	})
}
