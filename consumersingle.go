package wkafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

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

func (c *consumerSingle[T]) iterationConcurrent(ctx context.Context, cl *kgo.Client, fetch kgo.Fetches) error {
	for iter := fetch.RecordIter(); !iter.Done(); {
		r := iter.Next()
		// check partition is revoked if not then add to group
		if !c.PartitionHandler.IsRevokedRecord(r) {
			c.Group.Add(r)
		}

		if !iter.Done() && !c.Group.IsEnough() {
			continue
		}

		errGroup, ctxGroup := errgroup.WithContext(ctx)
		errGroup.SetLimit(c.Cfg.Concurrent.Process)

		c.Group.Merge()
		for records := range c.Group.Iter() {
			if c.Group.IsSingle() {
				for _, record := range records {
					errGroup.Go(func() error {
						if err := c.iterationMain(ctxGroup, record); err != nil {
							return wrapErr(record, err, c.IsDLQ)
						}

						return nil
					})
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
			return fmt.Errorf("wait group failed: %w", err)
		}

		// commit all records in group
		records := c.Group.AllRecords()
		records, _ = c.PartitionHandler.IsRevokedRecordBatch(records)
		if len(records) != 0 {
			if err := cl.CommitRecords(ctx, records...); err != nil {
				return fmt.Errorf("commit batch records failed: %w; offsets: %s", err, errorOffsetList(records))
			}
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
		if err := cl.CommitRecords(ctx, r); err != nil {
			return wrapErr(r, fmt.Errorf("commit records failed: %w", err), c.IsDLQ)
		}
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
