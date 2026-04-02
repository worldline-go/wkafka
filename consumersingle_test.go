package wkafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/worldline-go/wkafka/mocks"
	"go.uber.org/mock/gomock"
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

func TestConsumerSingle_iterationConcurrent(t *testing.T) {
	ctrl := gomock.NewController(t)

	for _, tt := range []struct {
		name           string
		consumer       consumerSingle[any]
		fetches        kgo.Fetches
		mockClientFunc func(mockclient *mocks.Mockclient)
		expectedError  bool
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
			fetches: oneRecordTestInput,
			mockClientFunc: func(mockclient *mocks.Mockclient) {
				record := oneRecordTestInput[0].Topics[0].Partitions[0].Records[0]
				mockclient.EXPECT().PauseFetchPartitions(map[string][]int32{
					record.Topic: {record.Partition},
				})
				mockclient.EXPECT().SetOffsets(map[string]map[int32]kgo.EpochOffset{
					record.Topic: {
						record.Partition: kgo.NewOffset().At(record.Offset).EpochOffset(),
					},
				})
				mockclient.EXPECT().ResumeFetchPartitions(map[string][]int32{
					record.Topic: {record.Partition},
				})
			},
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
					return errors.New("some error")
				},
			},
			fetches: oneRecordTestInput,
			mockClientFunc: func(mockclient *mocks.Mockclient) {
				record := oneRecordTestInput[0].Topics[0].Partitions[0].Records[0]
				mockclient.EXPECT().PauseFetchPartitions(map[string][]int32{
					record.Topic: {record.Partition},
				})
				mockclient.EXPECT().SetOffsets(map[string]map[int32]kgo.EpochOffset{
					record.Topic: {
						record.Partition: kgo.NewOffset().At(record.Offset).EpochOffset(),
					},
				})
				mockclient.EXPECT().ResumeFetchPartitions(map[string][]int32{
					record.Topic: {record.Partition},
				})
			},
			expectedError: false,
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
			fetches: oneRecordTestInput,
			mockClientFunc: func(mockclient *mocks.Mockclient) {
				record := oneRecordTestInput[0].Topics[0].Partitions[0].Records[0]
				mockclient.EXPECT().PauseFetchPartitions(map[string][]int32{
					record.Topic: {record.Partition},
				})
				mockclient.EXPECT().SetOffsets(map[string]map[int32]kgo.EpochOffset{
					record.Topic: {
						record.Partition: kgo.NewOffset().At(record.Offset).EpochOffset(),
					},
				})
				mockclient.EXPECT().ResumeFetchPartitions(map[string][]int32{
					record.Topic: {record.Partition},
				})
			},
			expectedError: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mockClient := mocks.NewMockclient(ctrl)

			if tt.mockClientFunc != nil {
				tt.mockClientFunc(mockClient)
			}

			err := tt.consumer.iterationConcurrent(t.Context(), mockClient, tt.fetches)
			if !tt.expectedError {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestConsumerSingle_iterationRecords(t *testing.T) {
	t.Run("consumer stops when parent context is cancelled, nothing is committed", func(t *testing.T) {
		ctrl := gomock.NewController(t)

		processed := make([]any, 0, 20)
		cs := consumerSingle[any]{
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

		mockClient := mocks.NewMockclient(ctrl)

		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			time.Sleep(500 * time.Millisecond)
			cancel()
		}()

		err := cs.iterationConcurrent(ctx, mockClient, fetch)

		require.Error(t, err)
		require.ErrorContains(t, err, "main consumer context error")

		require.LessOrEqual(t, len(processed), 20, "at most 20 events should be processed")
	})
}

func TestConsumerSingle_Consume(t *testing.T) {
	ctrl := gomock.NewController(t)

	t.Run("partition is rewound after processing error", func(t *testing.T) {
		mockClient := mocks.NewMockclient(ctrl)

		cs := consumerSingle[any]{
			customer: &customer[any]{
				Cfg: &ConsumerConfig{
					Concurrent: ConcurrentConfig{
						Enabled: true,
						Process: 5,
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
				var r struct {
					Error string `json:"error"`
				}

				require.NoError(t, json.Unmarshal(msg.([]byte), &r))

				if r.Error != "" {
					return errors.New(r.Error)
				}

				return nil
			},
		}

		recordsPar0 := getNTestRecords(0, 10, 60)
		recordsPar1 := getNTestRecords(1, 100, 60)

		rewindTargetRecord := recordsPar1[10]

		mockClient.EXPECT().AllowRebalance().AnyTimes()
		mockClient.EXPECT().PollRecords(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ int) kgo.Fetches {
			// Poll first batch of messages from both partitions, partition 1 returns error while processing the last message
			par1 := append([]*kgo.Record{}, recordsPar1...)
			par1[19] = &kgo.Record{
				Topic:     "test",
				Partition: 1,
				Value:     []byte(`{"error": "random error"}`),
			}

			return getTestFetches(recordsPar0[0:20], par1)
		})

		// Three groups of records should get committed, the last one fails.
		mockClient.EXPECT().MarkCommitRecords(recordsPar0[0:10])
		mockClient.EXPECT().MarkCommitRecords(recordsPar0[10:20])
		mockClient.EXPECT().MarkCommitRecords(recordsPar1[0:10])

		// Last 10 records of partition 1 are not committed and the partition is set to be rewound.
		mockClient.EXPECT().PauseFetchPartitions(map[string][]int32{
			rewindTargetRecord.Topic: {rewindTargetRecord.Partition},
		})
		mockClient.EXPECT().SetOffsets(map[string]map[int32]kgo.EpochOffset{
			rewindTargetRecord.Topic: {
				rewindTargetRecord.Partition: kgo.NewOffset().At(rewindTargetRecord.Offset).EpochOffset(),
			},
		})
		mockClient.EXPECT().ResumeFetchPartitions(map[string][]int32{
			rewindTargetRecord.Topic: {rewindTargetRecord.Partition},
		})

		// The next returned buffer was already buffered when SetOffsets was called. The partition 1 records should be ignored.
		mockClient.EXPECT().PollRecords(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ int) kgo.Fetches {
			// Verify that partition 1 is rewinding
			require.True(t, cs.PartitionHandler.isPartitionRewinding("test", 1))

			return getTestFetches(recordsPar0[20:40], recordsPar1[20:40])
		})

		// Only partition 0 is committed.
		mockClient.EXPECT().MarkCommitRecords(recordsPar0[20:30])
		mockClient.EXPECT().MarkCommitRecords(recordsPar0[30:40])

		// New fetch is consumed with rewound offset on partition 1 and it's processed successfully this time
		mockClient.EXPECT().PollRecords(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ int) kgo.Fetches {
			// Verify that partition 1 is rewinding
			require.True(t, cs.PartitionHandler.isPartitionRewinding("test", 1))

			return getTestFetches(recordsPar0[40:60], recordsPar1[10:30])
		})

		// Only partition 0 is committed.
		mockClient.EXPECT().MarkCommitRecords(recordsPar0[40:50])
		mockClient.EXPECT().MarkCommitRecords(recordsPar0[50:60])
		mockClient.EXPECT().MarkCommitRecords(recordsPar1[10:20])
		mockClient.EXPECT().MarkCommitRecords(recordsPar1[20:30])

		// Return fetches with error to close consumer
		mockClient.EXPECT().PollRecords(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, _ int) kgo.Fetches {
			// Verify that partition 1 is not rewinding anymore
			require.False(t, cs.PartitionHandler.isPartitionRewinding("test", 1))

			return kgo.Fetches{
				{
					Topics: []kgo.FetchTopic{
						{
							Partitions: []kgo.FetchPartition{
								{
									Err: kgo.ErrClientClosed,
								},
							},
						},
					},
				},
			}
		})

		err := cs.Consume(t.Context(), mockClient)
		require.ErrorIs(t, err, errClientClosed)

		require.False(t, cs.PartitionHandler.isPartitionRewinding("test", 1))
	})

	t.Run("partition is revoked while a partition is being rewound", func(t *testing.T) {
		mockClient := mocks.NewMockclient(ctrl)

		cs := consumerSingle[any]{
			customer: &customer[any]{
				Cfg: &ConsumerConfig{
					Concurrent: ConcurrentConfig{
						Enabled: true,
						Process: 5,
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
				var r struct {
					Error string `json:"error"`
				}

				require.NoError(t, json.Unmarshal(msg.([]byte), &r))

				if r.Error != "" {
					return errors.New(r.Error)
				}

				return nil
			},
		}

		mockClient.EXPECT().AllowRebalance().AnyTimes()

		// First batch of records returns error, partition starts rewinding.
		recordsPar0 := getNTestRecords(0, 10, 20)
		recordsPar0[8].Value = []byte(`{"error": "random error"}`)
		rewindTargetRecord := recordsPar0[0]
		mockClient.EXPECT().PollRecords(gomock.Any(), gomock.Any()).Return(getTestFetches(recordsPar0))

		// Partition is set to be rewound.
		mockClient.EXPECT().PauseFetchPartitions(map[string][]int32{
			rewindTargetRecord.Topic: {rewindTargetRecord.Partition},
		})
		mockClient.EXPECT().SetOffsets(map[string]map[int32]kgo.EpochOffset{
			rewindTargetRecord.Topic: {
				rewindTargetRecord.Partition: kgo.NewOffset().At(rewindTargetRecord.Offset).EpochOffset(),
			},
		})
		mockClient.EXPECT().ResumeFetchPartitions(map[string][]int32{
			rewindTargetRecord.Topic: {rewindTargetRecord.Partition},
		})

		// Second batch is ignored, because partition is set for rewinding
		recordsPar0 = getNTestRecords(0, 30, 20)
		mockClient.EXPECT().PollRecords(gomock.Any(), gomock.Any()).Return(getTestFetches(recordsPar0))

		// Some partition is revoked during the next poll records causing dropping all buffered fetches
		mockClient.EXPECT().PollRecords(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, i int) kgo.Fetches {
			// Verify that partition 0 is rewinding
			require.True(t, cs.PartitionHandler.isPartitionRewinding("test", 0))

			revokedCallback := partitionRevoked(cs.PartitionHandler, func(fn ...OptionDLQTriggerFn) {
				return
			})
			revokedCallback(t.Context(), nil, map[string][]int32{
				"test": {1},
			})

			return nil
		})

		recordsPar0 = getNTestRecords(0, 30, 20)
		mockClient.EXPECT().PollRecords(gomock.Any(), gomock.Any()).Return(getTestFetches(recordsPar0))
		mockClient.EXPECT().MarkCommitRecords(recordsPar0[0:10])
		mockClient.EXPECT().MarkCommitRecords(recordsPar0[10:20])

		// Return fetches with error to close consumer
		mockClient.EXPECT().PollRecords(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, i int) kgo.Fetches {
			// Verify that partition 0 is not rewinding
			require.False(t, cs.PartitionHandler.isPartitionRewinding("test", 0))

			return kgo.Fetches{
				{
					Topics: []kgo.FetchTopic{
						{
							Partitions: []kgo.FetchPartition{
								{
									Err: kgo.ErrClientClosed,
								},
							},
						},
					},
				},
			}
		})

		err := cs.Consume(t.Context(), mockClient)
		require.ErrorIs(t, err, errClientClosed)
	})
}

func getNTestRecords(
	partition int32,
	startOffset int64,
	numRecords int,
) []*kgo.Record {
	records := make([]*kgo.Record, numRecords)
	for i := range numRecords {
		records[i] = &kgo.Record{
			Topic:     "test",
			Partition: partition,
			Offset:    startOffset + int64(i),
			Value:     []byte(`{}`),
		}
	}

	return records
}

func getTestFetches(partition ...[]*kgo.Record) kgo.Fetches {
	partitions := make([]kgo.FetchPartition, len(partition))
	for i := range partition {
		partitions[i] = kgo.FetchPartition{
			Partition: partition[i][0].Partition,
			Records:   partition[i],
		}
	}

	return kgo.Fetches{
		{
			Topics: []kgo.FetchTopic{
				{
					Topic:      "test",
					Partitions: partitions,
				},
			},
		},
	}
}
