package wkafka

import (
	"context"
	"time"
)

type waitRetry struct {
	Interval time.Duration
}

func (w *waitRetry) CurrentInterval() time.Duration {
	return w.Interval
}

func (w *waitRetry) Sleep(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(w.Interval):
		return nil
	}
}
