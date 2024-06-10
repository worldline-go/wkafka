package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"connectrpc.com/grpcreflect"
	"github.com/worldline-go/initializer"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/errgroup"

	"github.com/worldline-go/wkafka"
	"github.com/worldline-go/wkafka/handler"
	"github.com/worldline-go/wkafka/handler/gen/wkafka/wkafkaconnect"
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

func RunExampleSingle(ctx context.Context, _ *sync.WaitGroup) error {
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

func RunExampleSingleWithHandler(ctx context.Context, _ *sync.WaitGroup) error {
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

	mux := http.NewServeMux()
	mux.Handle(handler.New(client))

	reflector := grpcreflect.NewStaticReflector(wkafkaconnect.WkafkaServiceName)

	mux.Handle(grpcreflect.NewHandlerV1(reflector))
	mux.Handle(grpcreflect.NewHandlerV1Alpha(reflector))

	s := http.Server{
		Addr:    ":8080",
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	initializer.Shutdown.Add(s.Close)

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
