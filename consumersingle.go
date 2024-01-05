package wkafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type consumerSingle[T any] struct {
	Process func(ctx context.Context, msg T) error
	Cfg     ConsumerConfig
	Decode  func(raw []byte, r *kgo.Record) (T, error)
	// PreCheck is a function that is called before the callback and decode.
	PreCheck   func(ctx context.Context, r *kgo.Record) error
	Option     optionConsumer
	ProduceDLQ func(ctx context.Context, err error, records []*kgo.Record) error
	Skip       func(cfg *ConsumerConfig, r *kgo.Record) bool
	IsDLQ      bool
}

func (c *consumerSingle[T]) setPreCheck(fn func(ctx context.Context, r *kgo.Record) error) {
	c.PreCheck = fn
}

func (c *consumerSingle[T]) Consume(ctx context.Context, cl *kgo.Client) error {
	for {
		fetch := cl.PollRecords(ctx, c.Cfg.MaxPollRecords)
		if fetch.IsClientClosed() {
			return ErrClientClosed
		}

		if err := fetch.Err(); err != nil {
			return fmt.Errorf("poll fetch err: %w", err)
		}

		// TODO check is needed?
		if fetch.Empty() {
			continue
		}

		if !c.Option.Concurrent {
			if err := c.iteration(ctx, cl, fetch); err != nil {
				return err
			}

			continue
		}

		if err := c.concurrentIteration(ctx, cl, fetch); err != nil {
			return err
		}
	}
}

/////////////////////////////////
// SINGLE - CONCURRENT ITERATION
/////////////////////////////////

func (c *consumerSingle[T]) concurrentIteration(ctx context.Context, cl *kgo.Client, fetch kgo.Fetches) error {
	return nil
}

////////////////////
// SINGLE - ITERATION
////////////////////

func (c *consumerSingle[T]) iteration(ctx context.Context, cl *kgo.Client, fetch kgo.Fetches) error {
	for iter := fetch.RecordIter(); !iter.Done(); {
		r := iter.Next()

		if err := c.iterationRecord(ctx, r); err != nil {
			// send to DLQ if enabled
			if c.ProduceDLQ != nil && isDQLError(err) {
				if err := c.ProduceDLQ(ctx, err, []*kgo.Record{r}); err != nil {
					return wrapErr(r, fmt.Errorf("produce to DLQ failed: %w", err), c.IsDLQ)
				}
			} else {
				return wrapErr(r, err, c.IsDLQ)
			}
		}

		if err := cl.CommitRecords(ctx, r); err != nil {
			return wrapErr(r, fmt.Errorf("commit records failed: %w", err), c.IsDLQ)
		}
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
	if err := c.Process(ctxCallback, data); err != nil {
		return err
	}

	return nil
}
