package wkafka_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/worldline-go/test/container/containerkafka"
	"github.com/worldline-go/test/utils/kafkautils"

	"github.com/worldline-go/wkafka"
)

type Data struct {
	Test int `json:"test"`
}

func (d Data) ProduceHook(r *wkafka.Record) {
	r.Key = fmt.Append(nil, d.Test)
}

type Processor struct {
	ClientID string
	Wait     time.Duration
	CallBack func(ctx context.Context, msg Data) error
	T        *testing.T
}

func (p Processor) SetWait(d time.Duration) Processor {
	p.Wait = d

	return p
}
func (p Processor) SetClientID(id string) Processor {
	p.ClientID = id

	return p
}

func (p Processor) SetTesting(t *testing.T) Processor {
	p.T = t

	return p
}

func (p Processor) Process(ctx context.Context, msg Data) error {
	record := wkafka.CtxRecord(ctx)
	p.T.Log("callback", "client_id", p.ClientID, "test", msg.Test, "topic", record.Topic, "key", string(record.Key))

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

	v := c.Map[msg]
	c.Map[msg] = v + 1

	if c.IsFinish != nil && c.IsFinish(c.Map) {
		if c.Close != nil {
			c.Close()
		}
	}

	return nil
}

func Test_GroupConsuming(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	container := containerkafka.New(t)
	defer container.Stop(t)

	ctx := t.Context()

	topic := kafkautils.Topic{Name: "test_group_consuming", Partitions: 3}
	if _, err := container.CreateTopics(ctx, topic); err != nil {
		t.Fatalf("CreateTopics() error = %v", err)
	}

	t.Log("topic created topic", topic.Name)

	byteProducer, err := wkafka.NewProducer[Data](container.Client, topic.Name)
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
		AppName     string
		ClientID    string
		Config      wkafka.ConsumerConfig
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
					AppName:  "app_1",
					ClientID: "test-1",
					Config: wkafka.ConsumerConfig{
						Topics:  []string{topic.Name},
						GroupID: "test-consuming",
					},
				},
				{
					AppName:  "app_2",
					ClientID: "test-2",
					Config: wkafka.ConsumerConfig{
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
					AppName:  "app_1",
					ClientID: "test-after-1",
					Config: wkafka.ConsumerConfig{
						Topics:  []string{topic.Name},
						GroupID: "test-consuming-after",
					},
					MessageWait: 2 * time.Second,
				},
				{
					AppName:  "app_2",
					ClientID: "test-after-2",
					Config: wkafka.ConsumerConfig{
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

	t.Log("all messages produced")

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
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
				Process  func(ctx context.Context, msg Data) error
				Wait     time.Duration
			}

			clients := make([]clientHold, 0, len(tt.consumers))
			for _, c := range tt.consumers {
				process := p.SetWait(c.MessageWait).SetClientID(c.ClientID).SetTesting(t)
				client, err := wkafka.New(
					ctx,
					container.Config,
					wkafka.WithClientID(c.ClientID),
					wkafka.WithAppName(c.AppName),
					wkafka.WithConsumer(c.Config),
				)
				if err != nil {
					t.Fatalf("NewClient() error = %v", err)
				}

				defer func(clientID string) {
					t.Log("client closing", "client_id", clientID)
					client.Close()
				}(c.ClientID)

				t.Log(
					"client created",
					"client_id", c.ClientID,
					"group_id", c.Config.GroupID,
					"topics", c.Config.Topics,
					"wait", c.Wait,
					"message_wait", c.MessageWait,
				)

				clients = append(clients, clientHold{
					Client:   client,
					Process:  process.Process,
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
						t.Log("finish wait", "wait", cPass.Wait, "client_id", cPass.ClientID)
					}

					t.Log("start consuming", "client_id", cPass.ClientID)
					if err := cPass.Client.Consume(ctx, wkafka.WithCallback(cPass.Process)); err != nil && !errors.Is(err, context.Canceled) {
						t.Errorf("Consume() error = %v", err)
					}
				}(c)
			}

			t.Log("wait for consuming")
			wg.Wait()

			for k, v := range counter.Map {
				t.Log("count", "key", k, "value", v)
				if v != 1 && k != 0 {
					t.Errorf("invalid count: %d", v)
				}
			}
		})
	}
}
