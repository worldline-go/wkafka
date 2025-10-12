package consumer

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"

	"github.com/worldline-go/wkafka"
	"github.com/worldline-go/wkafka/plugins/handler"
)

func getPort() string {
	if v := os.Getenv("PORT"); v != "" {
		return ":" + v
	}

	return ":8080"
}

var (
	kafkaConfigSingle = wkafka.Config{
		Brokers: []string{"localhost:9092"},
		Consumer: wkafka.ConsumerPreConfig{
			FormatDLQTopic: "finops_{{.AppName}}_dlq",
		},
		Plugins: map[string]any{
			handler.PluginName: map[string]any{
				"enabled": true,
				"addr":    getPort(),
				"pubsub": map[string]any{
					"prefix": "finops_",
					"redis": map[string]any{
						"address": "localhost:6379",
					},
				},
			},
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

func ProcessSingleWithTrace(tracer *kotel.Tracer) func(ctx context.Context, msg DataSingle) error {
	return func(ctx context.Context, msg DataSingle) error {
		ctx, span := tracer.WithProcessSpan(wkafka.CtxRecord(ctx))
		defer span.End()

		v, ok := ctx.Value("my-value").(string)
		slog.Info("my-value", slog.String("value", v), slog.Bool("ok", ok), slog.String("ctx", fmt.Sprintf("%s", ctx)))

		return ProcessSingle(ctx, msg)
	}
}

func RunExampleSingle(ctx context.Context) error {
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

	if err := client.Consume(ctx, wkafka.WithCallback(ProcessSingle)); err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	return nil
}

func RunExampleSingleWithTrace(ctx context.Context) error {
	kafkaTracer := kotel.NewTracer()

	client, err := wkafka.New(
		ctx, kafkaConfigSingle,
		wkafka.WithConsumer(consumeConfigSingle),
		wkafka.WithClientInfo("testapp", "v0.1.0"),
		wkafka.WithKGOOptions(kgo.WithHooks(kotel.NewKotel(kotel.WithTracer(kafkaTracer)).Hooks()...)),
		wkafka.WithLogger(slog.Default()),
	)
	if err != nil {
		return err
	}

	defer client.Close()

	ctx = context.WithValue(ctx, "my-value", "test")

	if err := client.Consume(ctx, wkafka.WithCallback(ProcessSingleWithTrace(kafkaTracer))); err != nil {
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
		wkafka.WithPlugin(handler.PluginWithName()),
	)
	if err != nil {
		return err
	}

	defer client.Close()

	return client.Consume(ctx, wkafka.WithCallback(ProcessSingle))
}
