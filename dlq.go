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
	Err     error
}

type dlqProcess[T any] struct {
	customer *customer[T]

	isRevokedRecord func(r *kgo.Record) bool
	setDLQRecord    func(r *kgo.Record, t time.Time, err error)
	callTrigger     func(ctx context.Context)
	processDLQ      func(ctx context.Context, msg T) error

	checkFunc      func(opts []OptionDLQTriggerFn)
	checkFuncMutex sync.Mutex
}

func newDLQProcess[T any](
	c *customer[T],
	isRevokedRecord func(r *kgo.Record) bool,
	dlqRecord func(r *kgo.Record, t time.Time, err error),
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

func (d *dlqProcess[T]) Iteration(ctx context.Context, cl *kgo.Client, fetch kgo.Fetches) error {
	for iter := fetch.RecordIter(); !iter.Done(); {
		r := iter.Next()
		// check partition is revoked
		if d.isRevokedRecord(r) {
			continue
		}

		// listening DLQ topics
		// check partition is revoked and not commit it!
		// when error return than it will not be committed
		if err := d.iteration(ctx, r); err != nil {
			if errors.Is(err, errPartitionRevoked) {
				// don't commit revoked partition
				// above check also skip others on that partition
				continue
			}

			return wrapErr(r, err, true)
		}

		// commit if not see any error
		cl.MarkCommitRecords(r)
	}

	return nil
}

func (d *dlqProcess[T]) iterationRecordDLQ(ctx context.Context, r *kgo.Record) error {
	if d.customer.Skip(d.customer.Cfg, r) {
		d.customer.Logger.Info("record skipped", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset)

		return nil
	}

	if d.customer.PreCheck != nil {
		if err := d.customer.PreCheck(ctx, r); err != nil {
			if errors.Is(err, ErrSkip) {
				d.customer.Logger.Info("record skipped", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "error", err)
				return nil
			}

			return fmt.Errorf("pre check failed: %w", err)
		}
	}

	data, err := d.customer.Decode(r.Value, r)
	if err != nil {
		if errors.Is(err, ErrSkip) {
			d.customer.Logger.Info("record skipped", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "error", err)
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
func (d *dlqProcess[T]) iteration(ctx context.Context, r *kgo.Record) error {
	wait := newWaitRetry(d.customer.Cfg.DLQ.RetryInterval, d.customer.Cfg.DLQ.RetryMaxInterval)
	defer wait.Close()

	firstIteration := true
	defer func() {
		d.setDLQRecord(nil, time.Time{}, nil)
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
			var errOrgDefault error
			if ok {
				errOrgDefault = errOrg.Err
				// error could be just in index
				if errOrgDefault == nil && len(errOrg.Indexes) == 1 {
					for _, err := range errOrg.Indexes {
						errOrgDefault = err

						break
					}
				}
			} else {
				// fail listening DLQ, if it is not DLQ error
				return err
			}

			errWrapped = wrapErr(r, errOrgDefault, true)

			d.customer.Logger.Error("DLQ process failed", "error", errWrapped, "retry_interval", wait.CurrentInterval().Truncate(time.Second).String())

			d.setDLQRecord(r, time.Now().Add(wait.CurrentInterval()), errOrgDefault)

			if firstIteration {
				d.setCheckFunc(func(opts []OptionDLQTriggerFn) {
					o := &OptionDLQTrigger{}
					for _, opt := range opts {
						opt(o)
					}

					if o.Force {
						wait.Trigger()

						return
					}

					if o.Spec != nil {
						if r.Topic == o.Spec.Topic && r.Partition == o.Spec.Partition && r.Offset == o.Spec.Offset {
							wait.Trigger()

							return
						}

						return
					}

					if o.SpecPartitions != nil {
						if partitions, ok := o.SpecPartitions[r.Topic]; ok {
							for _, partition := range partitions {
								if r.Partition == partition {
									wait.Trigger()

									return
								}
							}
						}

						return
					}

					if d.customer.Skip(d.customer.Cfg, r) {
						wait.Trigger()

						return
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

func (d *dlqProcess[T]) Trigger(opts []OptionDLQTriggerFn) {
	d.checkFuncMutex.Lock()
	defer d.checkFuncMutex.Unlock()

	if d.checkFunc != nil {
		d.checkFunc(opts)
	}
}

func (d *dlqProcess[T]) setCheckFunc(fn func(opts []OptionDLQTriggerFn)) {
	d.checkFuncMutex.Lock()
	defer d.checkFuncMutex.Unlock()

	d.checkFunc = fn
}

// ////////////////////////////////////////////////////////////////////////////

type DLQTriggerSpec struct {
	Topic     string `cfg:"topic"     json:"topic"`
	Partition int32  `cfg:"partition" json:"partition"`
	Offset    int64  `cfg:"offset"    json:"offset"`
}

type OptionDLQTrigger struct {
	Force          bool               `cfg:"force"           json:"force"`
	Spec           *DLQTriggerSpec    `cfg:"spec"            json:"spec"`
	SpecPartitions map[string][]int32 `cfg:"spec_partitions" json:"spec_partitions"`
}

func (o *OptionDLQTrigger) ToOption() OptionDLQTriggerFn {
	return func(opt *OptionDLQTrigger) {
		opt.Force = o.Force
		opt.Spec = o.Spec
	}
}

type OptionDLQTriggerFn func(*OptionDLQTrigger)

func WithDLQTriggerForce() OptionDLQTriggerFn {
	return func(o *OptionDLQTrigger) {
		o.Force = true
	}
}

func WithDLQTriggerSpec(specs *DLQTriggerSpec) OptionDLQTriggerFn {
	return func(o *OptionDLQTrigger) {
		o.Spec = specs
	}
}

func WithDLQTriggerSpecPartitions(partitions map[string][]int32) OptionDLQTriggerFn {
	return func(o *OptionDLQTrigger) {
		o.SpecPartitions = partitions
	}
}
