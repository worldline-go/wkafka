package wkafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

type consumerBatch[T any] struct {
	*customer[T]

	// Process is nil for DLQ consumer.
	Process          func(ctx context.Context, msg []T) error
	IsDLQ            bool
	Group            groupRecords
	PartitionHandler *partitionHandler
	DLQProcess       *dlqProcess[T]
}

func (c *consumerBatch[T]) setPreCheck(fn func(ctx context.Context, r *kgo.Record) error) {
	c.PreCheck = fn
}

func (c *consumerBatch[T]) Consume(ctx context.Context, cl *kgo.Client) error {
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
			if err := c.batchIterationConcurrent(ctx, cl, fetch); err != nil {
				return err
			}

			continue
		}

		if err := c.batchIteration(ctx, cl, fetch); err != nil {
			return err
		}
	}
}

/////////////////////////////////
// BATCH - ITERATION
/////////////////////////////////

func (c *consumerBatch[T]) batchIterationConcurrent(ctx context.Context, cl *kgo.Client, fetch kgo.Fetches) error {
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
		records := records // capture loop variable
		errGroup.Go(func() error {
			batch := make([]T, 0, len(records))
			batchRecords := make([]*kgo.Record, 0, len(records))

			for _, record := range records {
				// skip precheck and record section
				/////////////////////////////////
				data, skip, err := c.batchRecord(ctxGroup, record)
				if err != nil {
					return err
				}

				if !skip {
					// add to batch if record is not skipped
					batch = append(batch, data)
					batchRecords = append(batchRecords, record)
				}
			}

			/////////////////////////////////
			ctxCallback := context.WithValue(ctxGroup, KeyRecord, batchRecords)
			if err := c.Process(ctxCallback, batch); err != nil {
				errOrg, ok := IsDQLError(err)
				if !ok {
					// it is not DLQ error, return it
					// this will fail the service
					return fmt.Errorf("process batch failed: %w; offsets: %s", err, errorOffsetList(batchRecords))
				}

				if c.ProduceDLQ != nil {
					if err := c.ProduceDLQ(ctxGroup, errOrg, batchRecords); err != nil {
						return fmt.Errorf("produce to DLQ failed: %w; offsets: %s", err, errorOffsetList(batchRecords))
					}
				} else {
					// returning a batch error could be confusing
					return fmt.Errorf("process batch failed: %w; offsets: %s", err, errorOffsetList(batchRecords))
				}
			}

			return nil
		})
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

func (c *consumerBatch[T]) batchIteration(ctx context.Context, cl *kgo.Client, fetch kgo.Fetches) error {
	// get the batch count but not more than the number of records
	batchCount := c.Cfg.BatchCount
	if v := fetch.NumRecords(); v < batchCount {
		batchCount = v
	}

	// batch to process
	batch := make([]T, 0, batchCount)
	// records is used to commit
	records := make([]*kgo.Record, 0, batchCount)
	// batchRecords is used to add to context so callback can see the records when needed
	batchRecords := make([]*kgo.Record, 0, batchCount)
	for iter := fetch.RecordIter(); !iter.Done(); {
		r := iter.Next()

		// ignore revoked partitions in our fetch, don't commit them
		if !c.PartitionHandler.IsRevokedRecord(r) {
			// add to records to commit, with skip or not
			records = append(records, r)

			// skip precheck and record section
			/////////////////////////////////
			data, skip, err := c.batchRecord(ctx, r)
			if err != nil {
				return err
			}

			if !skip {
				// add to batch if record is not skipped
				batch = append(batch, data)
				batchRecords = append(batchRecords, r)
			}
		}
		/////////////////////////////////

		// fill the batch but if we are last record then process it
		if !iter.Done() && len(batch) < batchCount {
			continue
		}

		// add records to context so callback can see the records when needed
		ctxCallback := context.WithValue(ctx, KeyRecord, batchRecords)
		if err := c.Process(ctxCallback, batch); err != nil {
			errOrg, ok := IsDQLError(err)
			if !ok {
				// it is not DLQ error, return it
				// this will fail the service
				return fmt.Errorf("process batch failed: %w; offsets: %s", err, errorOffsetList(batchRecords))
			}

			if c.ProduceDLQ != nil {
				if err := c.ProduceDLQ(ctx, errOrg, batchRecords); err != nil {
					return fmt.Errorf("produce to DLQ failed: %w; offsets: %s", err, errorOffsetList(batchRecords))
				}
			} else {
				// returning a batch error could be confusing
				return fmt.Errorf("process batch failed: %w; offsets: %s", err, errorOffsetList(batchRecords))
			}
		}

		// if partitions are revoked then don't commit, filter out revoked records
		records, _ = c.PartitionHandler.IsRevokedRecordBatch(records)

		if len(records) != 0 {
			if err := cl.CommitRecords(ctx, records...); err != nil {
				return fmt.Errorf("commit batch records failed: %w; offsets: %s", err, errorOffsetList(records))
			}
		}

		// Instead of allocating new slices, reuse the existing slices by reslicing to zero length.
		// This avoids unnecessary allocations and keeps the underlying arrays.
		batch = batch[:0]
		batchRecords = batchRecords[:0]
		records = records[:0]
	}

	return nil
}

// batchRecord returns true if the record was skipped, false if it was processed.
func (c *consumerBatch[T]) batchRecord(ctx context.Context, r *kgo.Record) (T, bool, error) {
	var zero T

	if c.Skip(c.Cfg, r) {
		c.Logger.Debug("record skipped", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset)

		return zero, true, nil
	}

	if c.PreCheck != nil {
		if err := c.PreCheck(ctx, r); err != nil {
			if errors.Is(err, ErrSkip) {
				c.Logger.Debug("record skipped on precheck", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "error", err)

				return zero, true, nil
			}

			return zero, false, fmt.Errorf("pre check failed: %w", wrapErr(r, err, false))
		}
	}

	data, err := c.Decode(r.Value, r)
	if err != nil {
		if errors.Is(err, ErrSkip) {
			c.Logger.Info("record skipped on decode", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "error", err)

			return zero, true, nil
		}

		return zero, false, fmt.Errorf("decode record failed: %w", wrapErr(r, err, false))
	}

	return data, false, nil
}
