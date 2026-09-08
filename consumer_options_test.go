package wkafka

import (
	"context"
	"errors"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCallbackRunSizeDefaults(t *testing.T) {
	for _, tt := range []struct {
		name                      string
		typ                       string
		enabled                   bool
		single                    bool
		batch, process, run, want int
	}{
		{name: "mix batch", typ: "mix", enabled: true, batch: 2, process: 3, want: 6},
		{name: "negative run", typ: "mix", enabled: true, batch: 2, process: 3, run: -1, want: 6},
		{name: "default batch and process", typ: "mix", enabled: true, want: DefaultBatchCount * 10},
		{name: "explicit equal to old default", typ: "mix", enabled: true, batch: 2, process: 3, run: 2, want: 2},
		{name: "explicit override", typ: "mix", enabled: true, batch: 2, process: 3, run: 5, want: 5},
		{name: "explicit smaller", typ: "mix", enabled: true, batch: 2, process: 3, run: 1, want: 1},
		{name: "single mix", typ: "mix", enabled: true, single: true, batch: 2, process: 3, want: 2},
		{name: "key batch", typ: "key", enabled: true, batch: 2, process: 3, want: 2},
		{name: "partition batch", typ: "partition", enabled: true, batch: 2, process: 3, want: 2},
		{name: "default type", enabled: true, batch: 2, process: 3, want: 2},
		{name: "disabled mix", typ: "mix", batch: 2, process: 3, want: 2},
		{name: "one process", typ: "mix", enabled: true, batch: 2, process: 1, want: 2},
		{name: "one process explicit", typ: "mix", enabled: true, batch: 2, process: 1, run: 6, want: 6},
		{name: "negative process", typ: "mix", enabled: true, batch: 2, process: -1, want: 20},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(t.Context(), Config{Brokers: []string{"127.0.0.1:1"}}, WithPing(false), WithConsumer(ConsumerConfig{
				GroupID: "test", BatchCount: tt.batch,
				Concurrent: ConcurrentConfig{Enabled: tt.enabled, Type: tt.typ, Process: tt.process, RunSize: tt.run},
			}))
			require.NoError(t, err)
			defer c.Close()
			baseRunSize := c.consumerGroup.RunSize
			o := optionConsumer{Client: c, ConsumerConfig: c.consumerConfig}
			var records groupRecords
			if tt.single {
				require.NoError(t, WithCallback(func(context.Context, []byte) error { return nil })(&o))
				records = o.Consumer.(*consumerSingle[[]byte]).Group
			} else {
				require.NoError(t, WithCallbackBatch(func(context.Context, [][]byte) error { return nil })(&o))
				records = o.Consumer.(*consumerBatch[[]byte]).Group
			}
			for i := 0; i < tt.want; i++ {
				require.False(t, records.IsEnough())
				records.Add(&kgo.Record{})
			}
			require.True(t, records.IsEnough())
			// Batch construction must not change the defaults for a subsequent single consumer.
			require.Equal(t, baseRunSize, c.consumerGroup.RunSize)
		})
	}
}

func TestConsumerOneProcessGrouping(t *testing.T) {
	for _, typ := range []string{"key", "partition", "mix"} {
		for _, single := range []bool{false, true} {
			name := typ + "/batch"
			if single {
				name = typ + "/single"
			}
			t.Run(name, func(t *testing.T) {
				c, err := New(t.Context(), Config{Brokers: []string{"127.0.0.1:1"}}, WithPing(false), WithConsumer(ConsumerConfig{
					GroupID: "test", BatchCount: 2,
					Concurrent: ConcurrentConfig{Enabled: true, Type: typ, Process: 1, RunSize: 6},
				}))
				require.NoError(t, err)
				defer c.Close()
				require.Equal(t, 1, c.consumerConfig.Concurrent.Process)
				records := make([]*kgo.Record, 6)
				for i := range records {
					records[i] = &kgo.Record{Topic: "test", Partition: int32(i / 3), Offset: int64(i % 3), Key: []byte{byte(i % 2)}, Value: []byte{byte(i)}}
				}
				var active atomic.Int32
				var mu sync.Mutex
				var batches [][][]byte
				callback := func(_ context.Context, batch [][]byte) error {
					if active.Add(1) != 1 {
						t.Error("Process = 1 allowed overlapping callbacks")
					}
					defer active.Add(-1)
					runtime.Gosched()
					mu.Lock()
					batches = append(batches, append([][]byte(nil), batch...))
					mu.Unlock()
					return nil
				}
				o := optionConsumer{Client: c, ConsumerConfig: c.consumerConfig}
				if single {
					require.NoError(t, WithCallback(func(ctx context.Context, value []byte) error {
						return callback(ctx, [][]byte{value})
					})(&o))
				} else {
					require.NoError(t, WithCallbackBatch(callback)(&o))
				}
				stop := errors.New("stop polling")
				polls := 0
				cl := &recordingClient{poll: func(context.Context, int) kgo.Fetches {
					polls++
					if polls == 1 {
						return testSingleFetch(records[:3], records[3:])
					}
					return kgo.Fetches{{Topics: []kgo.FetchTopic{{Partitions: []kgo.FetchPartition{{Err: stop}}}}}}
				}}
				require.ErrorIs(t, o.Consumer.Consume(t.Context(), cl), stop)
				// Grouped single consumers also mark the whole run, not individual records.
				require.Equal(t, [][]*kgo.Record{records}, cl.marked)
				if single {
					require.Len(t, batches, 6)
					var values [][]byte
					for _, batch := range batches {
						require.Len(t, batch, 1)
						values = append(values, batch[0])
					}
					if typ == "mix" {
						require.Equal(t, [][]byte{{0}, {1}, {2}, {3}, {4}, {5}}, values)
					} else {
						batches = [][][]byte{values[:3], values[3:]}
					}
				}
				switch typ {
				case "key":
					require.ElementsMatch(t, [][][]byte{{{0}, {2}, {4}}, {{1}, {3}, {5}}}, batches)
				case "partition":
					require.ElementsMatch(t, [][][]byte{{{0}, {1}, {2}}, {{3}, {4}, {5}}}, batches)
				case "mix":
					if !single {
						require.Equal(t, [][][]byte{{{0}, {1}}, {{2}, {3}}, {{4}, {5}}}, batches)
					}
				}
			})
		}
	}
}

func TestMixBatchDefaultRunSizeOverflow(t *testing.T) {
	for _, run := range []int{0, 1} {
		c, err := New(t.Context(), Config{Brokers: []string{"127.0.0.1:1"}}, WithPing(false), WithConsumer(ConsumerConfig{
			GroupID: "test", BatchCount: math.MaxInt/2 + 1,
			Concurrent: ConcurrentConfig{Enabled: true, Type: "mix", Process: 2, RunSize: run},
		}))
		require.NoError(t, err)
		o := optionConsumer{Client: c, ConsumerConfig: c.consumerConfig}
		err = WithCallbackBatch(func(context.Context, [][]byte) error { return nil })(&o)
		c.Close()
		if run == 0 {
			require.EqualError(t, err, "validate consumer config: default concurrent run_size overflows int: batch_count * process")
		} else {
			require.NoError(t, err)
		}
	}
}

func TestMixBatchDefaultCallbacksOverlap(t *testing.T) {
	c, err := New(t.Context(), Config{Brokers: []string{"127.0.0.1:1"}}, WithPing(false), WithConsumer(ConsumerConfig{
		GroupID: "test", BatchCount: 2,
		Concurrent: ConcurrentConfig{Enabled: true, Type: "mix", Process: 3},
	}))
	require.NoError(t, err)
	defer c.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	started := make(chan int, 3)
	release := make(chan struct{})
	o := optionConsumer{Client: c, ConsumerConfig: c.consumerConfig}
	require.NoError(t, WithCallbackBatch(func(ctx context.Context, batch [][]byte) error {
		started <- len(batch)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})(&o))
	records := make([]*kgo.Record, 6)
	for i := range records {
		records[i] = &kgo.Record{Topic: "test", Offset: int64(i)}
	}
	cl := &recordingClient{}
	done := make(chan error, 1)
	go func() {
		done <- o.Consumer.(*consumerBatch[[]byte]).batchIterationConcurrent(ctx, cl, testSingleFetch(records))
	}()
	for range 3 {
		select {
		case size := <-started:
			if size != 2 {
				t.Errorf("batch size = %d, want 2", size)
			}
		case <-ctx.Done():
			<-done
			t.Fatal("three batch callbacks did not overlap")
		}
	}
	close(release)
	require.NoError(t, <-done)
	require.Equal(t, [][]*kgo.Record{records}, cl.marked)
}

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
