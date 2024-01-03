package main

import (
	"context"
	"sync"

	"github.com/worldline-go/initializer"
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

func (d *Data) ProduceHook(r *wkafka.Record) {
	r.Value = []byte(d.Name)
	r.Headers = append(r.Headers, wkafka.Header{
		Key:   "name",
		Value: []byte(d.Name),
	})
	r.Key = []byte(d.Name)
	r.Topic = d.Topic
}

func main() {
	initializer.Init(run)
}

func run(ctx context.Context, _ *sync.WaitGroup) error {
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

	producer, err := wkafka.NewProducer(client, wkafka.ProducerConfig[*Data]{})
	if err != nil {
		return err
	}

	return producer.Produce(ctx, data)
}
