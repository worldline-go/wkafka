package wkafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type consumerSingle[T any] struct {
	Process          func(ctx context.Context, msg T) error
	Cfg              ConsumeConfig
	Decode           func(raw []byte) (T, error)
	DecodeWithRecord func(raw []byte, r *kgo.Record) (T, error)
	// PreCheck is a function that is called before the callback and decode.
	PreCheck func(ctx context.Context, r *kgo.Record) error
}

func (c consumerSingle[T]) config() ConsumeConfig {
	return c.Cfg
}

func (c consumerSingle[T]) Consume(ctx context.Context, cl *kgo.Client) error {
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
			if err := c.singleIteration(ctx, cl, fetch); err != nil {
				return err
			}

			continue
		}
	}
}

/////////////////////////////////
// SINGLE - CONCURRENT ITERATION
/////////////////////////////////

func (c consumerSingle[T]) singleConcurrentIteration(ctx context.Context, cl *kgo.Client, fetch kgo.Fetches) error {
	return nil
}

////////////////////
// SINGLE ITERATION
////////////////////

func (c consumerSingle[T]) singleIteration(ctx context.Context, cl *kgo.Client, fetch kgo.Fetches) error {
	for iter := fetch.RecordIter(); !iter.Done(); {
		r := iter.Next()
		if !skip(&c.Cfg, r) {
			if err := c.singleIterationRecord(ctx, r); err != nil {
				return wrapErr(r, err)
			}
		}

		if err := cl.CommitRecords(ctx, r); err != nil {
			return wrapErr(r, fmt.Errorf("commit records failed: %w", err))
		}
	}

	return nil
}

func (c consumerSingle[T]) singleIterationRecord(ctx context.Context, r *kgo.Record) error {
	if c.PreCheck != nil {
		if err := c.PreCheck(ctx, r); err != nil {
			if errors.Is(err, ErrSkip) {
				return nil
			}

			return fmt.Errorf("pre check failed: %w", err)
		}
	}

	var data T
	if c.DecodeWithRecord != nil {
		var err error
		data, err = c.DecodeWithRecord(r.Value, r)
		if err != nil {
			if errors.Is(err, ErrSkip) {
				return nil
			}

			return fmt.Errorf("decode record with record failed: %w", err)
		}
	} else {
		var err error
		data, err = c.Decode(r.Value)
		if err != nil {
			if errors.Is(err, ErrSkip) {
				return nil
			}

			return fmt.Errorf("decode record failed: %w", err)
		}
	}

	ctxCallback := context.WithValue(ctx, KeyRecord, r)
	if err := c.Process(ctxCallback, data); err != nil {
		return fmt.Errorf("callback failed: %w", err)
	}

	return nil
}
