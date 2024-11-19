package wkafka

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v4"
)

type waitRetry struct {
	interval time.Duration
	Backoff  *backoff.ExponentialBackOff
}

func newWaitRetry(initialDuration, maxDuration time.Duration) *waitRetry {
	return &waitRetry{
		interval: initialDuration,
		Backoff: backoff.NewExponentialBackOff(
			backoff.WithInitialInterval(initialDuration),
			backoff.WithMaxInterval(maxDuration),
			backoff.WithMaxElapsedTime(0),
		),
	}
}

func (w *waitRetry) CurrentInterval() time.Duration {
	return w.interval
}

func (w *waitRetry) next() {
	w.interval = w.Backoff.NextBackOff()
}

func (w *waitRetry) Sleep(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(w.interval):
		w.next()
		return nil
	}
}
