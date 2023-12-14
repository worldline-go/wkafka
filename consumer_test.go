package wkafka_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/worldline-go/wkafka"
	"github.com/worldline-go/wkafka/tkafka"
)

type Data struct {
	Test int `json:"test"`
}

func (d Data) ProduceHook(r *wkafka.Record) {
	r.Key = []byte(fmt.Sprint(d.Test))
}

type Processor struct {
	ClientID string
	Wait     time.Duration
	CallBack func(ctx context.Context, msg Data) error
}

func (p Processor) SetWait(d time.Duration) Processor {
	p.Wait = d

	return p
}
func (p Processor) SetClientID(id string) Processor {
	p.ClientID = id

	return p
}

func (p Processor) Process(ctx context.Context, msg Data) error {
	record := wkafka.GetRecord(ctx)
	slog.Info("callback", slog.String("client_id", p.ClientID), slog.Any("test", msg.Test), slog.String("topic", record.Topic), slog.String("key", string(record.Key)))

	if p.Wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(p.Wait):
		}
	}

	p.CallBack(ctx, msg)

	return nil
}

type Counter[T comparable] struct {
	m sync.Mutex

	Map map[T]int64

	IsFinish func(map[T]int64) bool
	Close    func()
}

func (c *Counter[T]) Reset() {
	c.m.Lock()
	defer c.m.Unlock()

	c.Map = nil
}

func (c *Counter[T]) Count(ctx context.Context, msg T) error {
	c.m.Lock()
	defer c.m.Unlock()

	if c.Map == nil {
		c.Map = make(map[T]int64)
	}

	v, _ := c.Map[msg]
	c.Map[msg] = v + 1

	if c.IsFinish != nil && c.IsFinish(c.Map) {
		if c.Close != nil {
			c.Close()
		}
	}

	return nil
}

func Test_GroupConsuming(t *testing.T) {
	client, err := tkafka.TestClient()
	if err != nil {
		t.Fatalf("TestClient() error = %v", err)
	}

	defer client.Close()

	slog.Info("client connected")

	ctx := context.Background()

	gen := tkafka.NewGenerate(client)
	defer gen.Cleanup()

	topic := tkafka.Topic{Name: "test_group_consuming", Partitions: 3}
	if _, err := gen.CreateTopics(ctx, topic); err != nil {
		t.Fatalf("CreateTopics() error = %v", err)
	}

	slog.Info("topic created", slog.String("topic", topic.Name))

	byteProducer, err := wkafka.NewProducer(client, wkafka.ProducerConfig[Data]{
		Topic: topic.Name,
	})
	if err != nil {
		t.Fatalf("NewProducer() error = %v", err)
	}

	counter := Counter[int]{
		IsFinish: func(m map[int]int64) bool {
			// check 0 is consumed
			if _, ok := m[0]; ok {
				return true
			}

			return false
		},
	}

	type consumers struct {
		ClientID    string
		Config      wkafka.ConsumeConfig
		Wait        time.Duration
		MessageWait time.Duration
	}

	tests := []struct {
		name      string
		consumers []consumers
		wantErr   bool
	}{
		{
			name: "valid group consuming",
			consumers: []consumers{
				{
					ClientID: "test-1",
					Config: wkafka.ConsumeConfig{
						Topics:  []string{topic.Name},
						GroupID: "test-consuming",
					},
				},
				{
					ClientID: "test-2",
					Config: wkafka.ConsumeConfig{
						Topics:  []string{topic.Name},
						GroupID: "test-consuming",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "valid group consuming after",
			consumers: []consumers{
				{
					ClientID: "test-after-1",
					Config: wkafka.ConsumeConfig{
						Topics:  []string{topic.Name},
						GroupID: "test-consuming-after",
					},
					MessageWait: 2 * time.Second,
				},
				{
					ClientID: "test-after-2",
					Config: wkafka.ConsumeConfig{
						Topics:  []string{topic.Name},
						GroupID: "test-consuming-after",
					},
					Wait:        14 * time.Second,
					MessageWait: 2 * time.Second,
				},
			},
			wantErr: false,
		},
	}

	if err := byteProducer.Produce(ctx, []Data{
		{Test: 1},
		{Test: 2},
		{Test: 3},
		{Test: 4},
		{Test: 5},
		{Test: 6},
		{Test: 7},
		{Test: 8},
		{Test: 9},
		{Test: 0},
	}...); err != nil {
		t.Fatalf("Produce() error = %v", err)
	}

	slog.Info("all messages produced")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			counter.Reset()
			counter.Close = func() {
				cancel()
			}

			p := Processor{
				CallBack: func(ctx context.Context, msg Data) error {
					counter.Count(ctx, msg.Test)

					return nil
				},
			}

			type clientHold struct {
				Client   *wkafka.Client
				ClientID string
				Wait     time.Duration
			}

			clients := make([]clientHold, 0, len(tt.consumers))
			for _, c := range tt.consumers {
				process := p.SetWait(c.MessageWait).SetClientID(c.ClientID)
				client, err := wkafka.NewClient(
					ctx,
					tkafka.Config(),
					wkafka.WithClientID(c.ClientID),
					wkafka.WithConsumer(c.Config, process),
				)
				if err != nil {
					t.Fatalf("NewClient() error = %v", err)
				}

				defer func(clientID string) {
					slog.Info("client closing", slog.String("client_id", clientID))
					client.Close()
				}(c.ClientID)

				slog.Info(
					"client created",
					slog.String("client_id", c.ClientID),
					slog.String("group_id", c.Config.GroupID),
					slog.Any("topics", c.Config.Topics),
					slog.Duration("wait", c.Wait),
					slog.Duration("message_wait", c.MessageWait),
				)

				clients = append(clients, clientHold{
					Client:   client,
					ClientID: c.ClientID,
					Wait:     c.Wait,
				})
			}

			wg := new(sync.WaitGroup)
			for _, c := range clients {
				wg.Add(1)
				go func(cPass clientHold) {
					defer wg.Done()

					select {
					case <-ctx.Done():
						return
					case <-time.After(cPass.Wait):
						slog.Info("finish wait", slog.Duration("wait", cPass.Wait), slog.String("client_id", cPass.ClientID))
					}

					slog.Info("start consuming", slog.String("client_id", cPass.ClientID))
					if err := cPass.Client.Consume(ctx); err != nil && !errors.Is(err, context.Canceled) {
						t.Errorf("Consume() error = %v", err)
					}
				}(c)
			}

			slog.Info("wait for consuming")
			wg.Wait()

			for k, v := range counter.Map {
				slog.Info("count", slog.Int("key", k), slog.Int64("value", v))
				if v != 1 && k != 0 {
					t.Errorf("invalid count: %d", v)
				}
			}
		})
	}
}
