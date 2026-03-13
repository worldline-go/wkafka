package wkafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	oneRecordTestInput = kgo.Fetches{
		{
			Topics: []kgo.FetchTopic{
				{
					Topic: "test",
					Partitions: []kgo.FetchPartition{
						{
							Partition: 0,
							Records: []*kgo.Record{
								{
									Topic:     "test",
									Value:     []byte("test"),
									Partition: 0,
									Offset:    1,
								},
							},
						},
					},
				},
			},
		},
	}
)

type mockCommitter struct {
	committedOffsets []int64
}

func (m *mockCommitter) MarkCommitRecords(records ...*Record) {
	for _, record := range records {
		m.committedOffsets = append(m.committedOffsets, record.Offset)
	}
}

func TestConsumerSingle_iterationConcurrent(t *testing.T) {
	for _, tt := range []struct {
		name             string
		consumer         consumerSingle[any]
		fetches          kgo.Fetches
		committedOffsets []int64
		commiter         mockCommitter
		expectedError    bool
	}{
		{
			name: "consumer returns every processing error",
			consumer: consumerSingle[any]{
				customer: &customer[any]{
					Cfg: &ConsumerConfig{
						Concurrent: ConcurrentConfig{
							Process: 20,
						},
						Skip:                        make(map[string]map[int32]OffsetConfig),
						RecoverAfterProcessingError: false,
					},
					Skip: func(cfg *ConsumerConfig, r *kgo.Record) bool {
						return false
					},
					Decode: func(raw []byte, r *kgo.Record) (any, error) {
						return raw, nil
					},
					Logger: LogNoop{},
				},
				Group: newGroupMix(100, 10),
				PartitionHandler: &partitionHandler{
					logger: LogNoop{},
				},
				Process: func(ctx context.Context, msg any) error {
					return errors.New("some error")
				},
			},
			fetches:       oneRecordTestInput,
			commiter:      mockCommitter{[]int64{}},
			expectedError: true,
		},
		{
			name: "RecoverAfterProcessingError true, should only log processing error",
			consumer: consumerSingle[any]{
				customer: &customer[any]{
					Cfg: &ConsumerConfig{
						Concurrent: ConcurrentConfig{
							Process: 20,
						},
						Skip:                           make(map[string]map[int32]OffsetConfig),
						ProcessPartitionsIndependently: true,
						RecoverAfterProcessingError:    true,
					},
					Skip: func(cfg *ConsumerConfig, r *kgo.Record) bool {
						return false
					},
					Decode: func(raw []byte, r *kgo.Record) (any, error) {
						return raw, nil
					},
					Logger: LogNoop{},
				},
				Group: newGroupMix(100, 10),
				PartitionHandler: &partitionHandler{
					logger: LogNoop{},
				},
				Process: func(ctx context.Context, msg any) error {
					return errors.New("some error")
				},
			},
			fetches:          oneRecordTestInput,
			commiter:         mockCommitter{[]int64{}},
			expectedError:    false,
			committedOffsets: []int64{},
		},
		{
			name: "RecoverAfterProcessingError true, should return error when ErrFatal",
			consumer: consumerSingle[any]{
				customer: &customer[any]{
					Cfg: &ConsumerConfig{
						Concurrent: ConcurrentConfig{
							Process: 20,
						},
						Skip:                        make(map[string]map[int32]OffsetConfig),
						RecoverAfterProcessingError: true,
					},
					Skip: func(cfg *ConsumerConfig, r *kgo.Record) bool {
						return false
					},
					Decode: func(raw []byte, r *kgo.Record) (any, error) {
						return raw, nil
					},
					Logger: LogNoop{},
				},
				Group: newGroupMix(100, 10),
				PartitionHandler: &partitionHandler{
					logger: LogNoop{},
				},
				Process: func(ctx context.Context, msg any) error {
					return ErrFatal
				},
			},
			fetches:          oneRecordTestInput,
			commiter:         mockCommitter{[]int64{}},
			expectedError:    true,
			committedOffsets: []int64{},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.consumer.iterationConcurrent(t.Context(), &tt.commiter, tt.fetches)
			if !tt.expectedError {
				require.NoError(t, err)
				slices.Sort(tt.commiter.committedOffsets)

				require.Equal(t, tt.committedOffsets, tt.commiter.committedOffsets)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestConsumerSingle_iterationRecords(t *testing.T) {
	t.Run("consumer stops when parent context is cancelled", func(t *testing.T) {
		processed := make([]any, 0, 20)
		cs := consumerSingle[any]{
			customer: &customer[any]{
				Cfg: &ConsumerConfig{
					Concurrent: ConcurrentConfig{
						Process: 20,
					},
					Skip:                           make(map[string]map[int32]OffsetConfig),
					RecoverAfterProcessingError:    false,
					ProcessPartitionsIndependently: true,
				},
				Skip: func(cfg *ConsumerConfig, r *kgo.Record) bool {
					return false
				},
				Decode: func(raw []byte, r *kgo.Record) (any, error) {
					return raw, nil
				},
				Logger: LogNoop{},
			},
			Group: newGroupMix(100, 20),
			PartitionHandler: &partitionHandler{
				logger: LogNoop{},
			},
			Process: func(ctx context.Context, msg any) error {
				processed = append(processed, msg)
				time.Sleep(time.Second)

				return nil
			},
		}

		fetch := kgo.Fetches{
			{
				Topics: []kgo.FetchTopic{
					{
						Topic: "test",
						Partitions: []kgo.FetchPartition{
							{
								Partition: 0,
								Records:   make([]*kgo.Record, 30),
							},
						},
					},
				},
			},
		}

		for i := range 30 {
			fetch[0].Topics[0].Partitions[0].Records[i] = &kgo.Record{
				Topic: "test",
				Value: []byte(fmt.Sprintf("%d", i)),
			}
		}

		com := mockCommitter{
			committedOffsets: []int64{},
		}

		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			time.Sleep(500 * time.Millisecond)
			cancel()
		}()

		err := cs.iterationConcurrent(ctx, &com, fetch)

		require.Error(t, err)
		require.ErrorContains(t, err, "main consumer context error")

		require.LessOrEqual(t, len(processed), 20, "at most 20 events should be processed")
		require.Len(t, com.committedOffsets, 0, "no events should be committed")
	})
}
