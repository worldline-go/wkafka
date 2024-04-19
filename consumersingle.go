package wkafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/worldline-go/logz"
)

type consumerSingle[T any] struct {
	// Process is nil for DLQ consumer.
	Process func(ctx context.Context, msg T) error
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
	Meter            Meter
}

func (c *consumerSingle[T]) setPreCheck(fn func(ctx context.Context, r *kgo.Record) error) {
	c.PreCheck = fn
}

func (c *consumerSingle[T]) Consume(ctx context.Context, cl *kgo.Client) error {
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

		if err := c.iteration(ctx, cl, fetch); err != nil {
			return err
		}
	}
}

////////////////////
// SINGLE - ITERATION
////////////////////

func (c *consumerSingle[T]) iteration(ctx context.Context, cl *kgo.Client, fetch kgo.Fetches) error {
	for iter := fetch.RecordIter(); !iter.Done(); {
		r := iter.Next()

		// check partition is revoked
		if c.PartitionHandler.IsRevokedRecord(r) {
			continue
		}

		start := time.Now()
		if c.IsDLQ {
			// listening DLQ topics
			// check partition is revoked and not commit it!
			// when error return than it will not be committed
			if err := c.iterationDLQ(ctx, r); err != nil {
				c.Meter.Meter(start, 1, r.Topic, err, true)
				if errors.Is(err, errPartitionRevoked) {
					// don't commit revoked partition
					// above check also skip others on that partition
					continue
				}
				return wrapErr(r, err, c.IsDLQ)
			} else {
				c.Meter.Meter(start, 1, r.Topic, nil, true)
			}
		} else {
			// listening main topics

			if err := c.iterationMain(ctx, r); err != nil {
				c.Meter.Meter(start, 1, r.Topic, err, false)
				return wrapErr(r, err, c.IsDLQ)
			} else {
				c.Meter.Meter(start, 1, r.Topic, nil, false)
			}

			// maybe working on that record is too long and partition is revoked
			// not commit it, mess up the commit offest
			// callback function need to be awere of getting same message again and just need skip it without error
			// also error is ok due to it will be push in DLQ
			if c.PartitionHandler.IsRevokedRecord(r) {
				continue
			}
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
func (c *consumerSingle[T]) iterationDLQ(ctx context.Context, r *kgo.Record) error {
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

		if err := c.iterationRecord(ctx, r); err != nil {
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

// iterationMain is used to listen main topics.
func (c *consumerSingle[T]) iterationMain(ctx context.Context, r *kgo.Record) error {
	if err := c.iterationRecord(ctx, r); err != nil {
		errDLQ, ok := isDQLError(err)
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
	if c.IsDLQ {
		ctxCallback = context.WithValue(ctxCallback, KeyIsDLQProcess, true)
		if err := c.ProcessDLQ(ctxCallback, data); err != nil {
			return err
		}
	} else {
		if err := c.Process(ctxCallback, data); err != nil {
			return err
		}
	}

	return nil
}
