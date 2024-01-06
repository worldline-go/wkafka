package wkafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/worldline-go/logz"
)

type consumerBatch[T any] struct {
	Process func(ctx context.Context, msg []T) error
	Cfg     ConsumerConfig
	Decode  func(raw []byte, r *kgo.Record) (T, error)
	// PreCheck is a function that is called before the callback and decode.
	PreCheck         func(ctx context.Context, r *kgo.Record) error
	Option           optionConsumer
	ProduceDLQ       func(ctx context.Context, err error, records []*kgo.Record) error
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

		if err := c.batchIteration(ctx, cl, fetch); err != nil {
			return err
		}
	}
}

/////////////////////////////////
// BATCH - ITERATION
/////////////////////////////////

func (c *consumerBatch[T]) batchIteration(ctx context.Context, cl *kgo.Client, fetch kgo.Fetches) error {
	batchCount := c.Cfg.BatchCount
	if v := fetch.NumRecords(); v < c.Cfg.BatchCount {
		batchCount = v
	}

	batch := make([]T, 0, batchCount)
	records := make([]*kgo.Record, 0, batchCount)
	batchRecords := make([]*kgo.Record, 0, batchCount)
	for iter := fetch.RecordIter(); !iter.Done(); {
		r := iter.Next()
		records = append(records, r)

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

		batch = append(batch, data)
		batchRecords = append(batchRecords, r)

		if !iter.Done() && len(batch) < batchCount {
			continue
		}

		ctxCallback := context.WithValue(ctx, KeyRecord, batchRecords)

		if err := c.Process(ctxCallback, batch); err != nil {
			// if c.ProduceDLQ != nil && isDQLError(err) {
			// 	if err := c.ProduceDLQ(ctx, err, []*kgo.Record{r}); err != nil {
			// 		return wrapErr(r, fmt.Errorf("produce to DLQ failed: %w", err), c.IsDLQ)
			// 	}
			// } else {
			// 	return wrapErr(r, err, c.IsDLQ)
			// }
		}

		if err := cl.CommitRecords(ctx, records...); err != nil {
			return fmt.Errorf("commit batch records failed: %w", err)
		}

		batch = make([]T, 0, batchCount)
		batchRecords = make([]*kgo.Record, 0, batchCount)
		records = make([]*kgo.Record, 0, batchCount)
	}

	return nil
}
