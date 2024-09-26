package producer

import (
	"context"

	"github.com/worldline-go/wkafka"
)

var (
	kafkaConfig = wkafka.Config{
		Brokers: []string{"localhost:9092"},
	}
)

type Data struct {
	Name    string
	Topic   string
	Details map[string]interface{}
}

func ProduceHook(d *Data, r *wkafka.Record) error {
	r.Value = []byte(d.Name)
	r.Headers = append(r.Headers, wkafka.Header{
		Key:   "name",
		Value: []byte(d.Name),
	})
	r.Key = []byte(d.Name)
	r.Topic = d.Topic

	return nil
}

func RunExampleHook(ctx context.Context) error {
	client, err := wkafka.New(ctx, kafkaConfig)
	if err != nil {
		return err
	}
	defer client.Close()

	data := &Data{
		Name:  "xxx",
		Topic: "test",
		Details: map[string]interface{}{
			"key": 1234,
		},
	}

	producer, err := wkafka.NewProducer[*Data](client, "test", wkafka.WithHook(ProduceHook))
	if err != nil {
		return err
	}

	return producer.Produce(ctx, data)
}
