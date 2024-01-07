package wkafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/worldline-go/logz"
)

type consumerBatch[T any] struct {
	// Process is nil for DLQ consumer.
	Process func(ctx context.Context, msg []T) error
	// ProcessDLQ is nil for main consumer.
	ProcessDLQ func(ctx context.Context, msg T) error
	Cfg        ConsumerConfig
	Decode     func(raw []byte, r *kgo.Record) (T, error)
	// PreCheck is a function that is called before the callback and decode.
	PreCheck         func(ctx context.Context, r *kgo.Record) error
	Option           optionConsumer
	ProduceDLQ       func(ctx context.Context, err *DLQError, records []*kgo.Record) error
	Skip             func(cfg *ConsumerConfig, r *kgo.Record) bool
	Logger           logz.Adapter
	PartitionHandler *partitionHandler
	IsDLQ            bool
}

func (c *consumerBatch[T]) setPreCheck(fn func(ctx context.Context, r *kgo.Record) error) {
	c.PreCheck = fn
}

func (c *consumerBatch[T]) Consume(ctx context.Context, cl *kgo.Client) error {
	for {
		// flush the partition handler, it will be ready next poll
		c.PartitionHandler.Flush()

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
			if err := c.batchIterationDLQ(ctx, cl, fetch); err != nil {
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

func (c *consumerBatch[T]) batchIteration(ctx context.Context, cl *kgo.Client, fetch kgo.Fetches) error {
	// get the batch count but not more than the number of records
	batchCount := c.Cfg.BatchCount
	if v := fetch.NumRecords(); v < c.Cfg.BatchCount {
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
		if c.PartitionHandler.IsRevokedRecord(r) {
			continue
		}

		// add to records to commit, with skip or not
		records = append(records, r)

		// skip precheck and record section
		/////////////////////////////////
		if c.Skip(&c.Cfg, r) {
			continue
		}

		if c.PreCheck != nil {
			if err := c.PreCheck(ctx, r); err != nil {
				if errors.Is(err, ErrSkip) {
					continue
				}

				return fmt.Errorf("pre check failed: %w", err)
			}
		}

		data, err := c.Decode(r.Value, r)
		if err != nil {
			if errors.Is(err, ErrSkip) {
				continue
			}

			return fmt.Errorf("decode record failed: %w", err)
		}
		/////////////////////////////////

		// add to batch if record is not skipped
		batch = append(batch, data)
		batchRecords = append(batchRecords, r)

		// fill the batch but if we are last record then process it
		if !iter.Done() && len(batch) < batchCount {
			continue
		}

		// add records to context so callback can see the records when needed
		ctxCallback := context.WithValue(ctx, KeyRecord, batchRecords)
		if err := c.Process(ctxCallback, batch); err != nil {
			errOrg, ok := isDQLError(err)
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
				return fmt.Errorf("commit batch records failed: %w", err)
			}
		}

		batch = make([]T, 0, batchCount)
		batchRecords = make([]*kgo.Record, 0, batchCount)
		records = make([]*kgo.Record, 0, batchCount)
	}

	return nil
}

// batchIterationDLQ is the same as single interation DLQ.
func (c *consumerBatch[T]) batchIterationDLQ(ctx context.Context, cl *kgo.Client, fetch kgo.Fetches) error {
	for iter := fetch.RecordIter(); !iter.Done(); {
		r := iter.Next()

		// check partition is revoked
		if c.PartitionHandler.IsRevokedRecord(r) {
			continue
		}

		// listening DLQ topics
		// check partition is revoked and not commit it!
		// when error return than it will not be committed
		if err := c.iterationDLQ(ctx, r); err != nil {
			if errors.Is(err, errPartitionRevoked) {
				// don't commit revoked partition
				// above check also skip others on that partition
				continue
			}

			return wrapErr(r, err, c.IsDLQ)
		}

		// commit if not see any error
		if err := cl.CommitRecords(ctx, r); err != nil {
			return wrapErr(r, fmt.Errorf("commit records failed: %w", err), c.IsDLQ)
		}
	}

	return nil
}

// iterationDLQ is used to listen DLQ topics, error usually comes from context cancellation.
// any kind of error will be retry with interval.
func (c *consumerBatch[T]) iterationDLQ(ctx context.Context, r *kgo.Record) error {
	wait := waitRetry{
		Interval: c.Cfg.DLQ.RetryInterval,
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if c.PartitionHandler.IsRevokedRecord(r) {
			return errPartitionRevoked
		}

		if err := c.iterationRecordDLQ(ctx, r); err != nil {
			errOrg, ok := isDQLError(err)
			var errWrapped error
			if ok {
				errWrapped = wrapErr(r, errOrg.Err, c.IsDLQ)
			} else {
				errWrapped = wrapErr(r, err, c.IsDLQ)
			}

			c.Logger.Error("DLQ process failed", "err", errWrapped, "retry_interval", wait.CurrentInterval().String())

			if err := wait.Sleep(ctx); err != nil {
				return err
			}

			continue
		}

		break
	}

	return nil
}

func (c *consumerBatch[T]) iterationRecordDLQ(ctx context.Context, r *kgo.Record) error {
	if c.Skip(&c.Cfg, r) {
		return nil
	}

	if c.PreCheck != nil {
		if err := c.PreCheck(ctx, r); err != nil {
			if errors.Is(err, ErrSkip) {
				return nil
			}

			return fmt.Errorf("pre check failed: %w", err)
		}
	}

	data, err := c.Decode(r.Value, r)
	if err != nil {
		if errors.Is(err, ErrSkip) {
			return nil
		}

		return fmt.Errorf("decode record failed: %w", err)
	}

	ctxCallback := context.WithValue(ctx, KeyRecord, r)
	ctxCallback = context.WithValue(ctxCallback, KeyIsDLQ, true)
	if err := c.ProcessDLQ(ctxCallback, data); err != nil {
		return err
	}

	return nil
}
