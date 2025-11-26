package demo

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/worldline-go/wkafka"
)

var KafkaConfigConsumerSingleErrorDLQ = wkafka.ConsumerConfig{
	Topics:  []string{"my-topic"},
	GroupID: "my-group",
	DLQ: wkafka.DLQConfig{
		Topic: "my-group-dlq",
	},
}

// EXAMPLE=demo_consume_single_error_dlq
func RunConsumeSingleErrorDLQ(ctx context.Context) error {
	client, err := wkafka.New(
		ctx,
		KafkaConfig,
		wkafka.WithConsumer(KafkaConfigConsumerSingleErrorDLQ),
		wkafka.WithClientInfo("demo_consume_single_error_dlq", "v0.1.0"),
		wkafka.WithLogger(slog.Default()),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	return client.Consume(ctx, wkafka.WithCallback(ConsumeCallbackErrorDLQ))
}

func ConsumeCallbackErrorDLQ(ctx context.Context, data *Event) error {
	if strings.Contains(data.Message, "error") {
		return wkafka.WrapErrDLQ(fmt.Errorf("error appears in message"))
	}

	return nil
}

// func ConsumeCallbackErrorDLQBatch(ctx context.Context, data []*Event) error {
// 	errs := &wkafka.DLQError{}
// 	for i, d := range data {
// 		if strings.Contains(d.Message, "error") {
// 			errs.SetIndex(i, wkafka.WrapErrDLQ(fmt.Errorf("error appears in message")))
// 		}
// 	}

// 	if errs.IsZero() {
// 		return nil
// 	}

// 	return errs
// }
