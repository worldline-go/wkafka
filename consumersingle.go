package wkafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

type committer interface {
	MarkCommitRecords(...*Record)
	PauseFetchPartitions(map[string][]int32) map[string][]int32
	SetOffsets(map[string]map[int32]kgo.EpochOffset)
	ResumeFetchPartitions(map[string][]int32)
}

type consumerSingle[T any] struct {
	*customer[T]

	// Process is nil for DLQ consumer.
	Process          func(ctx context.Context, msg T) error
	IsDLQ            bool
	Group            groupRecords
	PartitionHandler *partitionHandler
	DLQProcess       *dlqProcess[T]
}

func (c *consumerSingle[T]) setPreCheck(fn func(ctx context.Context, r *kgo.Record) error) {
	c.PreCheck = fn
}

func (c *consumerSingle[T]) Consume(ctx context.Context, cl *kgo.Client) error {
	for {
		// flush the partition handler, it will be ready next poll
		c.PartitionHandler.Flush()

		// if block on poll then allow rebalance
		cl.AllowRebalance()

		if c.PartitionHandler.ShouldResetRewind() {
			c.PartitionHandler.ResetRewind()
		}

		fetch := cl.PollRecords(ctx, c.Cfg.MaxPollRecords)
		if fetch.IsClientClosed() {
			return errClientClosed
		}

		if err := fetch.Err(); err != nil {
			return fmt.Errorf("poll fetch err: %w", err)
		}

		// TODO check is needed?
		if fetch.Empty() {
			continue
		}

		if c.IsDLQ {
			if err := c.DLQProcess.Iteration(ctx, cl, fetch); err != nil {
				return err
			}

			continue
		}

		if c.Cfg.Concurrent.Enabled && c.Cfg.Concurrent.Process > 1 {
			if err := c.iterationConcurrent(ctx, cl, fetch); err != nil {
				return err
			}

			continue
		}

		if err := c.iteration(ctx, cl, fetch); err != nil {
			return err
		}
	}
}

////////////////////
// SINGLE - ITERATION
////////////////////

func (c *consumerSingle[T]) iterationConcurrent(ctx context.Context, cl committer, fetch kgo.Fetches) error {
	// If we don't process partitions independently, we process all the records in the same loop. If any record fails,
	// all the rest of the records are discarded.
	if !c.Cfg.ProcessPartitionsIndependently {
		if err := c.iterationRecords(ctx, cl, fetch.RecordIter()); err != nil {
			return fmt.Errorf("error while processing records: %w", err)
		}

		return nil
	}

	// If we process partitions independently, we process records from different partitions in separate loops.
	// If a record fails to be processed, only the rest of the records from the same partition is discarded.
	for _, f := range fetch {
		for _, topic := range f.Topics {
			for _, partition := range topic.Partitions {
				singlePartitionFetch := &kgo.Fetches{
					{
						Topics: []kgo.FetchTopic{
							{
								Topic:   topic.Topic,
								TopicID: topic.TopicID,
								Partitions: []kgo.FetchPartition{
									partition,
								},
							},
						},
					},
				}

				err := c.iterationRecords(ctx, cl, singlePartitionFetch.RecordIter())
				if err != nil {
					// We don't want to restart the service and only continue to the next partition.
					if c.Cfg.RecoverAfterProcessingError && !errors.Is(err, ErrFatal) {
						c.Logger.Warn("skipping to the next partition",
							"error", err,
						)

						continue
					}

					return fmt.Errorf("error while processing partition: %w", err)
				}
			}
		}
	}

	return nil
}

func (c *consumerSingle[T]) iterationRecords(ctx context.Context, cl committer, recordIter *kgo.FetchesRecordIter) error {
	for !recordIter.Done() {
		r := recordIter.Next()

		if c.PartitionHandler.IsPartitionRewinding(r.Topic, r.Partition) {
			if c.PartitionHandler.ShouldSkipRecord(r) && !recordIter.Done() {
				continue
			}

			c.PartitionHandler.MarkPartitionRewound(r.Topic, r.Partition)
		}

		// check partition is revoked if not then add to group
		if !c.PartitionHandler.IsRevokedRecord(r) {
			c.Group.Add(r)
		}

		if !recordIter.Done() && !c.Group.IsEnough() {
			continue
		}

		errGroup, ctxGroup := errgroup.WithContext(ctx)
		errGroup.SetLimit(c.Cfg.Concurrent.Process)

		c.Group.Merge()
	loop:
		for records := range c.Group.Iter() {
			if c.Group.IsSingle() {
				for _, record := range records {
					select {
					// if one of the record in the group failed and context is cancelled, we don't want to run processing of the rest
					case <-ctxGroup.Done():
						break loop
					default:
						errGroup.Go(func() error {
							if err := c.iterationMain(ctxGroup, record); err != nil {
								return wrapErr(record, err, c.IsDLQ)
							}

							return nil
						})
					}
				}
			} else {
				errGroup.Go(func() error {
					for _, record := range records {
						if err := c.iterationMain(ctxGroup, record); err != nil {
							return wrapErr(record, err, c.IsDLQ)
						}
					}

					return nil
				})
			}
		}

		if err := errGroup.Wait(); err != nil {
			rewindRecord := c.Group.AllRecords()[0]
			cl.PauseFetchPartitions(map[string][]int32{
				r.Topic: {rewindRecord.Partition},
			})

			cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
				rewindRecord.Topic: {
					rewindRecord.Partition: kgo.NewOffset().At(rewindRecord.Offset).EpochOffset(),
				},
			})

			cl.ResumeFetchPartitions(map[string][]int32{
				rewindRecord.Topic: {rewindRecord.Partition},
			})

			c.PartitionHandler.RewindPartitionToUncommittedOffset(rewindRecord.Topic, rewindRecord.Partition, rewindRecord.Offset)

			return fmt.Errorf("wait group failed: %w", err)
		}

		// If the parent context was cancelled, the errGroup won't return any error, so we need to check the parent context
		// and return the error if not nil.
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("main consumer context error: %w", err)
		}

		// commit all records in group
		records := c.Group.AllRecords()
		records, _ = c.PartitionHandler.IsRevokedRecordBatch(records)
		if len(records) != 0 {
			cl.MarkCommitRecords(records...)
		}

		c.Group.Reset()
	}

	return nil
}

func (c *consumerSingle[T]) iteration(ctx context.Context, cl *kgo.Client, fetch kgo.Fetches) error {
	for iter := fetch.RecordIter(); !iter.Done(); {
		r := iter.Next()
		// check partition is revoked
		if c.PartitionHandler.IsRevokedRecord(r) {
			continue
		}

		// listening main topics
		if err := c.iterationMain(ctx, r); err != nil {
			return wrapErr(r, err, c.IsDLQ)
		}

		// maybe working on that record is too long and partition is revoked
		// not commit it, mess up the commit offest
		// callback function need to be awere of getting same message again and just need skip it without error
		// also error is ok due to it will be push in DLQ
		if c.PartitionHandler.IsRevokedRecord(r) {
			continue
		}

		// commit if not see any error
		cl.MarkCommitRecords(r)
	}

	return nil
}

// iterationMain is used to listen main topics.
func (c *consumerSingle[T]) iterationMain(ctx context.Context, r *kgo.Record) error {
	if err := c.iterationRecord(ctx, r); err != nil {
		errDLQ, ok := IsDQLError(err)
		if !ok {
			// it is not DLQ error, return it
			return err
		}

		// send to DLQ if enabled
		if c.ProduceDLQ != nil {
			if err := c.ProduceDLQ(ctx, errDLQ, []*kgo.Record{r}); err != nil {
				return fmt.Errorf("produce to DLQ failed: %w", err)
			}

			return nil
		}

		return err
	}

	return nil
}

func (c *consumerSingle[T]) iterationRecord(ctx context.Context, r *kgo.Record) error {
	if c.Skip(c.Cfg, r) {
		c.Logger.Debug("record skipped", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset)

		return nil
	}

	if c.PreCheck != nil {
		if err := c.PreCheck(ctx, r); err != nil {
			if errors.Is(err, ErrSkip) {
				c.Logger.Debug("record skipped on precheck", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "error", err)

				return nil
			}

			return fmt.Errorf("pre check failed: %w", err)
		}
	}

	data, err := c.Decode(r.Value, r)
	if err != nil {
		if errors.Is(err, ErrSkip) {
			c.Logger.Info("record skipped on decode", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "error", err)

			return nil
		}

		return fmt.Errorf("decode record failed: %w", err)
	}

	ctxCallback := context.WithValue(ctx, KeyRecord, r)
	if err := c.Process(ctxCallback, data); err != nil {
		return err
	}

	return nil
}
