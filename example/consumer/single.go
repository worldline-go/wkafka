package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/rakunlabs/into"
	"golang.org/x/sync/errgroup"

	"github.com/worldline-go/wkafka"
	"github.com/worldline-go/wkafka/handler"
)

var (
	kafkaConfigSingle = wkafka.Config{
		Brokers: []string{"localhost:9092"},
		Consumer: wkafka.ConsumerPreConfig{
			FormatDLQTopic: "finops_{{.AppName}}_dlq",
		},
	}
	consumeConfigSingle = wkafka.ConsumerConfig{
		Topics:  []string{"test"},
		GroupID: "test_single",
	}
)

type DataSingle struct {
	Test       int    `json:"test"`
	Timeout    string `json:"timeout"`
	IsErr      bool   `json:"is_err"`
	IsErrFatal bool   `json:"is_err_fatal"`
}

func ProcessSingle(_ context.Context, msg DataSingle) error {
	slog.Info("callback", slog.Any("test", msg.Test), slog.Bool("is_err", msg.IsErr))

	if duration, err := time.ParseDuration(msg.Timeout); err != nil {
		slog.Error("parse duration", "error", err.Error())
	} else {
		slog.Info("sleep", slog.Duration("duration", duration))
		time.Sleep(duration)
	}

	if msg.IsErrFatal {
		return fmt.Errorf("test fatal error %d", msg.Test)
	}

	if msg.IsErr {
		return wkafka.WrapErrDLQ(fmt.Errorf("test error %d", msg.Test))
	}

	return nil
}

func RunExampleSingle(ctx context.Context) error {
	client, err := wkafka.New(
		ctx, kafkaConfigSingle,
		wkafka.WithConsumer(consumeConfigSingle),
		wkafka.WithClientInfo("testapp", "v0.1.0"),
	)
	if err != nil {
		return err
	}

	defer client.Close()

	if err := client.Consume(ctx, wkafka.WithCallback(ProcessSingle)); err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	return nil
}

func RunExampleSingleWithHandler(ctx context.Context) error {
	client, err := wkafka.New(
		ctx, kafkaConfigSingle,
		wkafka.WithConsumer(consumeConfigSingle),
		wkafka.WithClientInfo("testapp", "v0.1.0"),
		wkafka.WithLogger(slog.Default()),
	)
	if err != nil {
		return err
	}

	defer client.Close()

	wkafkaHander, err := handler.New(client)
	if err != nil {
		return err
	}

	mux := http.NewServeMux()
	mux.Handle(wkafkaHander.Handler())

	s := http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	into.ShutdownAdd(s.Close, "http server")

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		slog.Info("started listening on :8080")

		go func() {
			<-ctx.Done()
			s.Close()
		}()

		return s.ListenAndServe()
	})

	g.Go(func() error {
		return client.Consume(ctx, wkafka.WithCallback(ProcessSingle))
	})

	return g.Wait()
}
