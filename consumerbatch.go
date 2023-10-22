package wkafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type consumerBatch[T any] struct {
	Process          func(ctx context.Context, msg []T) error
	Cfg              ConsumeConfig
	Decode           func(raw []byte) (T, error)
	DecodeWithRecord func(raw []byte, r *kgo.Record) (T, error)
	// PreCheck is a function that is called before the callback and decode.
	PreCheck func(ctx context.Context, r *kgo.Record) error
}

func (c consumerBatch[T]) config() ConsumeConfig {
	return c.Cfg
}

func (c consumerBatch[T]) Consume(ctx context.Context, cl *kgo.Client) error {
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

		if !c.Cfg.Concurrent {
			if err := c.batchIteration(ctx, cl, fetch); err != nil {
				return err
			}

			continue
		}
	}
}

func (c consumerBatch[T]) batchIteration(ctx context.Context, cl *kgo.Client, fetch kgo.Fetches) error {
	batch := make([]T, 0, c.Cfg.BatchCount)
	records := make([]*kgo.Record, 0, c.Cfg.BatchCount)
	for iter := fetch.RecordIter(); !iter.Done(); {
		r := iter.Next()
		records = append(records, r)

		if c.PreCheck != nil {
			if err := c.PreCheck(ctx, r); err != nil {
				if errors.Is(err, ErrSkip) {
					return nil
				}

				return fmt.Errorf("pre check failed: %w", err)
			}
		}

		var v T
		if c.DecodeWithRecord != nil {
			var err error
			v, err = c.DecodeWithRecord(r.Value, r)
			if err != nil {
				if errors.Is(err, ErrSkip) {
					continue
				}

				return fmt.Errorf("decode record with record failed: %w", err)
			}
		} else {
			var err error
			v, err = c.Decode(r.Value)
			if err != nil {
				if errors.Is(err, ErrSkip) {
					continue
				}

				return fmt.Errorf("decode record failed: %w", err)
			}
		}

		batch = append(batch, v)

		if len(batch) < c.Cfg.BatchCount {
			continue
		}

		if err := c.Process(ctx, batch); err != nil {
			return fmt.Errorf("process batch failed: %w", err)
		}

		batch = make([]T, 0, c.Cfg.BatchCount)
	}

	if err := cl.CommitRecords(ctx, records...); err != nil {
		return fmt.Errorf("commit batch records failed: %w", err)
	}

	return nil
}
