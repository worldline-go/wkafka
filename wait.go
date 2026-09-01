package wkafka

import (
	"context"
	"time"

	"github.com/cenkalti/backoff/v7"
)

type waitRetry struct {
	interval time.Duration
	backoff  *backoff.ExponentialBackOff

	ch chan struct{}
}

func newWaitRetry(initialDuration, maxDuration time.Duration) *waitRetry {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = initialDuration
	b.MaxInterval = maxDuration

	return &waitRetry{
		interval: initialDuration,
		backoff:  b,
		ch:       make(chan struct{}, 1),
	}
}

func (w *waitRetry) CurrentInterval() time.Duration {
	return w.interval
}

func (w *waitRetry) next() {
	w.interval = w.backoff.NextBackOff()
}

func (w *waitRetry) Sleep(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(w.interval):
		w.next()
		return nil
	case <-w.ch:
		// consume all remaining messages
		for {
			select {
			case <-w.ch:
			default:
				return nil
			}
		}
	}
}

func (w *waitRetry) Trigger() {
	select {
	case w.ch <- struct{}{}:
	default:
	}
}

func (w *waitRetry) Close() {
	close(w.ch)
}
