package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/worldline-go/wkafka"
)

func ProcessSingleByte(ctx context.Context, raw []byte) error {
	var msg DataSingle
	if err := json.Unmarshal(raw, &msg); err != nil {
		return err
	}

	return ProcessSingle(ctx, msg)
}

func RunExampleSingleByte(ctx context.Context, _ *sync.WaitGroup) error {
	client, err := wkafka.New(
		ctx, kafkaConfigSingle,
		wkafka.WithConsumer(consumeConfigSingle),
		wkafka.WithClientInfo("testapp", "v0.1.0"),
	)
	if err != nil {
		return err
	}

	defer client.Close()

	if err := client.Consume(ctx, wkafka.WithCallback(ProcessSingleByte)); err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	return nil
}
