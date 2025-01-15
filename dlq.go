package wkafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type DLQRecord struct {
	Record  *kgo.Record
	RetryAt time.Time
}

type dlqProcess[T any] struct {
	customer *customer[T]

	isRevokedRecord func(r *kgo.Record) bool
	setDLQRecord    func(r *kgo.Record, t time.Time)
	callTrigger     func(ctx context.Context)
	processDLQ      func(ctx context.Context, msg T) error

	checkFunc      func()
	checkFuncMutex sync.Mutex
}

func newDLQProcess[T any](
	c *customer[T],
	isRevokedRecord func(r *kgo.Record) bool,
	dlqRecord func(r *kgo.Record, t time.Time),
	callTrigger func(ctx context.Context),
	processDLQ func(ctx context.Context, msg T) error,
) *dlqProcess[T] {
	return &dlqProcess[T]{
		customer:        c,
		isRevokedRecord: isRevokedRecord,
		setDLQRecord:    dlqRecord,
		callTrigger:     callTrigger,
		processDLQ:      processDLQ,
	}
}

func (d *dlqProcess[T]) iterationRecordDLQ(ctx context.Context, r *kgo.Record) error {
	if d.customer.Skip(d.customer.Cfg, r) {
		d.customer.Logger.Info("record skipped", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset)

		return nil
	}

	if d.customer.PreCheck != nil {
		if err := d.customer.PreCheck(ctx, r); err != nil {
			if errors.Is(err, ErrSkip) {
				return nil
			}

			return fmt.Errorf("pre check failed: %w", err)
		}
	}

	data, err := d.customer.Decode(r.Value, r)
	if err != nil {
		if errors.Is(err, ErrSkip) {
			return nil
		}

		return fmt.Errorf("decode record failed: %w", err)
	}

	ctxCallback := context.WithValue(ctx, KeyRecord, r)
	ctxCallback = context.WithValue(ctxCallback, KeyIsDLQProcess, true)
	if err := d.processDLQ(ctxCallback, data); err != nil {
		return err
	}

	return nil
}

// Iteration is used to listen DLQ topics, error usually comes from context cancellation.
// Any kind of error will be retry with interval.
func (d *dlqProcess[T]) Iteration(ctx context.Context, r *kgo.Record) error {
	wait := newWaitRetry(d.customer.Cfg.DLQ.RetryInterval, d.customer.Cfg.DLQ.RetryMaxInterval)
	defer wait.Close()

	firstIteration := true
	defer func() {
		d.setDLQRecord(nil, time.Time{})
		d.setCheckFunc(nil)
		d.callTrigger(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if d.isRevokedRecord(r) {
			return errPartitionRevoked
		}

		if err := d.iterationRecordDLQ(ctx, r); err != nil {
			errOrg, ok := IsDQLError(err)
			var errWrapped error
			if ok {
				errWrapped = wrapErr(r, errOrg.Err, true)
			} else {
				errWrapped = wrapErr(r, err, true)
			}

			d.customer.Logger.Error("DLQ process failed", "error", errWrapped, "retry_interval", wait.CurrentInterval().Truncate(time.Second).String())

			d.setDLQRecord(r, time.Now().Add(wait.CurrentInterval()))

			if firstIteration {
				d.setCheckFunc(func() {
					if d.customer.Skip(d.customer.Cfg, r) {
						wait.Trigger()
					}
				})

				firstIteration = false
			}

			d.callTrigger(ctx)

			if err := wait.Sleep(ctx); err != nil {
				return err
			}

			continue
		}

		break
	}

	return nil
}

func (d *dlqProcess[T]) Trigger() {
	d.checkFuncMutex.Lock()
	defer d.checkFuncMutex.Unlock()

	if d.checkFunc != nil {
		d.checkFunc()
	}
}

func (d *dlqProcess[T]) setCheckFunc(fn func()) {
	d.checkFuncMutex.Lock()
	defer d.checkFuncMutex.Unlock()

	d.checkFunc = fn
}
