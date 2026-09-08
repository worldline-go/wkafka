package wkafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestDLQIterationSuccessDoesNotNotify(t *testing.T) {
	c := &Client{}
	notifications := 0
	stateChanges := 0
	d := newDLQProcess(&customer[string]{
		Cfg:    &ConsumerConfig{},
		Logger: LogNoop{},
		Skip:   func(*ConsumerConfig, *kgo.Record) bool { return false },
		Decode: func(raw []byte, _ *kgo.Record) (string, error) { return string(raw), nil },
	}, func(*kgo.Record) bool { return false }, func(r *kgo.Record, retryAt time.Time, err error) {
		stateChanges++
		c.setDLQRecord(r, retryAt, err)
	}, func(context.Context) {
		notifications++
	}, func(context.Context, string) error { return nil })

	for i := 0; i < 3; i++ {
		require.NoError(t, d.iteration(t.Context(), &kgo.Record{Topic: "dlq", Offset: int64(i)}))
	}
	require.Zero(t, notifications)
	require.Zero(t, stateChanges)
	require.Equal(t, DLQRecord{}, c.DLQRecord())
	require.Nil(t, d.checkFunc)
}

func TestDLQIterationFailureNotifications(t *testing.T) {
	for _, exit := range []string{"recovery", "cancellation", "revocation", "fatal"} {
		t.Run(exit, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			c := &Client{}
			r := &kgo.Record{Topic: "dlq", Partition: 1, Offset: 42}
			retryErr := errors.New("retry failed")
			fatalErr := errors.New("fatal failure")
			cfg := &ConsumerConfig{}
			cfg.DLQ.RetryInterval = time.Hour
			cfg.DLQ.RetryMaxInterval = time.Hour
			attempts := 0
			revoked := false
			var notifications []DLQRecord
			var d *dlqProcess[string]
			d = newDLQProcess(&customer[string]{
				Cfg:    cfg,
				Logger: LogNoop{},
				Skip:   func(*ConsumerConfig, *kgo.Record) bool { return false },
				Decode: func(raw []byte, _ *kgo.Record) (string, error) { return string(raw), nil },
			}, func(*kgo.Record) bool { return revoked }, c.setDLQRecord, func(context.Context) {
				state := c.DLQRecord()
				notifications = append(notifications, state)
				if state.Record == nil {
					require.Nil(t, d.checkFunc)
					return
				}
				require.Same(t, r, state.Record)
				require.ErrorIs(t, state.Err, retryErr)
				require.False(t, state.RetryAt.IsZero())
				require.NotNil(t, d.checkFunc)
				if len(notifications) == 1 {
					c.DLQRetry(WithDLQTriggerSpec(&DLQTriggerSpec{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset}))
					return
				}
				switch exit {
				case "cancellation":
					cancel()
				case "revocation":
					revoked = true
				}
				c.DLQRetry(WithDLQTriggerForce())
			}, func(context.Context, string) error {
				attempts++
				if attempts <= 2 {
					return WrapErrDLQ(retryErr)
				}
				if exit == "fatal" {
					return fatalErr
				}
				return nil
			})
			c.dlqRetryTrigger = d.Trigger

			err := d.iteration(ctx, r)
			switch exit {
			case "recovery":
				require.NoError(t, err)
			case "cancellation":
				require.ErrorIs(t, err, context.Canceled)
			case "revocation":
				require.ErrorIs(t, err, errPartitionRevoked)
			case "fatal":
				require.ErrorIs(t, err, fatalErr)
			}
			require.Len(t, notifications, 3)
			require.Equal(t, DLQRecord{}, notifications[2])
			require.Equal(t, DLQRecord{}, c.DLQRecord())
			require.Nil(t, d.checkFunc)
			c.DLQRetry(WithDLQTriggerForce())
			if exit == "recovery" {
				require.NoError(t, d.iteration(ctx, &kgo.Record{Topic: r.Topic, Offset: r.Offset + 1}))
				require.Len(t, notifications, 3)
			}
		})
	}
}

func TestOptionDLQTriggerToOptionCopiesSpecPartitions(t *testing.T) {
	partitions := map[string][]int32{"topic": {1, 2}}
	input := OptionDLQTrigger{SpecPartitions: partitions}
	output := OptionDLQTrigger{}

	input.ToOption()(&output)

	require.Equal(t, partitions, output.SpecPartitions)
}
