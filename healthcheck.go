package wkafka

import (
	"context"
	"errors"
	"fmt"
)

var ErrNoBrokers = errors.New("no Kafka broker address provided")

// NewHealthCheck creates new health check function for Kafka broker.
// The idea is to connect to the broker using provided configuration,
// if we cannot do this for any reason - check fails.
// This function can be used standalone or as a check function
// in github.com/hellofresh/health-go package.
func NewHealthCheck(cfg Config) func(context.Context) error {
	return func(ctx context.Context) error {
		if len(cfg.Brokers) == 0 {
			return ErrNoBrokers
		}
		cl, err := New(ctx, cfg)
		if err != nil {
			return fmt.Errorf("failed to connect to Kafka broker: %w", err)
		}
		cl.Close()

		return nil
	}
}