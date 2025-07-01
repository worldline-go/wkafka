package wkafka_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/suite"
	"github.com/worldline-go/logz"
	"github.com/worldline-go/test/container/containerkafka"
	"github.com/worldline-go/test/utils/kafkautils"
	"golang.org/x/sync/errgroup"

	"github.com/worldline-go/wkafka"
)

type ConsumerSuite struct {
	suite.Suite
	container *containerkafka.Container
}

func (s *ConsumerSuite) SetupSuite() {
	s.container = containerkafka.New(s.T())
}

func TestConsumer(t *testing.T) {
	suite.Run(t, new(ConsumerSuite))
}

func (s *ConsumerSuite) TearDownSuite() {
	s.container.Stop(s.T())
}

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

func (s *ConsumerSuite) GroupConsuming() {
	if testing.Short() {
		s.T().Skip("skipping test in short mode")
	}

	topic := kafkautils.Topic{Name: "test_group_consuming", Partitions: 3}
	s.container.CreateTopics(s.T(), topic)

	s.T().Log("topic created topic", topic.Name)

	byteProducer, err := wkafka.NewProducer[Data](s.container.Client, topic.Name)
	if err != nil {
		s.T().Fatalf("NewProducer() error = %v", err)
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

	if err := byteProducer.Produce(s.T().Context(), []Data{
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
		s.T().Fatalf("Produce() error = %v", err)
	}

	s.T().Log("all messages produced")

	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
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
					s.container.Config,
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

func (s *ConsumerSuite) TestConsumerDLQ() {
	testName := strings.ReplaceAll(s.T().Name(), "/", "-")

	testMessages := []any{
		[]byte("test-message-1"),
		[]byte("test-message-2"),
		[]byte("test-message-3"),
		[]byte("test-message-4"),
		[]byte("test-message-5"),
		[]byte("test-message-6"),
	}
	s.container.Publish(s.T(), testName, testMessages...)

	// ////////////////////////////////////////////////////////////////////////////////////
	// Main consumer
	// ////////////////////////////////////////////////////////////////////////////////////

	lMain, err := wkafka.New(
		s.T().Context(), s.container.Config,
		wkafka.WithConsumer(wkafka.ConsumerConfig{
			GroupID: "test-group",
			Topics:  []string{testName},
			DLQ: wkafka.DLQConfig{
				ConsumerDisabled: true,
				Topic:            testName + "-dlq",
			},
		}),
	)
	s.NoError(err)

	errFail := errors.New("fail error")

	lMainConsumeTimes := 5
	lMainFunc := func(ctx context.Context, message []byte) error {
		lMainConsumeTimes--
		s.T().Log("callback", "message", string(message))
		if lMainConsumeTimes < 0 {
			s.Fail("too many messages")
			return nil
		}

		switch string(message) {
		case "test-message-1":
			return wkafka.ErrDLQ
		case "test-message-2":
			return nil
		case "test-message-3":
			return wkafka.WrapErrDLQ(fmt.Errorf("test error"))
		case "test-message-4":
			return wkafka.WrapErrDLQ(fmt.Errorf("test error"))
		case "test-message-5":
			return errFail
		case "test-message-6":
			return wkafka.WrapErrDLQ(fmt.Errorf("test error"))
		default:
			s.Fail("unexpected message")
			return nil
		}
	}

	// ////////////////////////////////////////////////////////////////////////////////////
	// DLQ consumer
	// ////////////////////////////////////////////////////////////////////////////////////

	lDLQ, err := wkafka.New(
		s.T().Context(), s.container.Config,
		wkafka.WithConsumer(wkafka.ConsumerConfig{
			GroupID: "test-group",
			DLQ: wkafka.DLQConfig{
				Topic: testName + "-dlq",
			},
		}),
	)
	s.NoError(err)

	lDLQConsumeTimes := 3
	lDLQFunc := func(ctx context.Context, message []byte) error {
		lDLQConsumeTimes--
		s.T().Log("callback", "message", string(message))
		if lDLQConsumeTimes < 0 {
			s.Fail("too many messages")
			return nil
		}

		s.Equal(testName+"-dlq", wkafka.CtxRecord(ctx).Topic)

		switch string(message) {
		case "test-message-1":
			return nil
		case "test-message-3":
			return nil
		case "test-message-4":
			return errFail
		default:
			s.Fail("unexpected message")
			return nil
		}
	}

	errGroup, ctx := errgroup.WithContext(s.T().Context())
	errGroup.Go(func() error {
		err := lMain.Consume(ctx, wkafka.WithCallback(lMainFunc))
		s.ErrorIs(err, errFail)
		return nil
	})

	errGroup.Go(func() error {
		err := lDLQ.Consume(ctx, wkafka.WithCallback(lDLQFunc))
		s.ErrorIs(err, errFail)
		return nil
	})

	s.NoError(errGroup.Wait())

	s.Equal(lMainConsumeTimes, 0, "main consumer consume times")
	s.Equal(lDLQConsumeTimes, 0, "dlq consumer consume times")
}

func (s *ConsumerSuite) TestConsumerRebalance() {
	s.T().Skip("skipping test, it is flaky")

	testName := strings.ReplaceAll(s.T().Name(), "/", "-")
	s.container.Admin.CreateTopic(s.T().Context(), 3, 1, nil, testName)

	errGroup, ctx := errgroup.WithContext(s.T().Context())

	errGroup.Go(func() error {
		index := 0
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			index++

			testMessages := []any{}
			for i := 1; i <= 3; i++ {
				testMessages = append(testMessages, wkafka.Record{
					Value:     []byte(fmt.Sprintf("test-part-%d-%d", index%3, i*index)),
					Partition: int32(index % 3),
				})
			}

			s.container.Publish(s.T(), testName, testMessages...)

			time.Sleep(20 * time.Second) // Wait for messages to be produced
		}
	})

	// ////////////////////////////////////////////////////////////////////////////////////
	// Main consumer
	// ////////////////////////////////////////////////////////////////////////////////////

	// errFail := errors.New("fail error")

	kafka1Process := func(ctx context.Context, message []byte) error {
		switch string(message) {
		// case "test-part-1-2":
		// 	return fmt.Errorf("kafka-1 message [%s]: %w", string(message), errFail)
		default:
			s.T().Log("kafka-1", "message", string(message))
			time.Sleep(2 * time.Minute) // Simulate long processing
			s.T().Log("kafka-1", "processed", string(message))
			return nil
		}
	}

	kafka2Process := func(ctx context.Context, message []byte) error {
		switch string(message) {
		// case "test-part-1-2":
		// 	return fmt.Errorf("kafka-2 message [%s]: %w", string(message), errFail)
		default:
			s.T().Log("kafka-2", "message", string(message))
			time.Sleep(2 * time.Second) // Simulate long processing
			s.T().Log("kafka-2", "processed", string(message))
			return nil
		}
	}

	errGroup.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			logger := log.With().Str("consumer", "kafka-1").Logger()
			kafka1, err := wkafka.New(
				ctx, s.container.Config,
				wkafka.WithConsumer(wkafka.ConsumerConfig{
					GroupID:        "test-group-rebalance",
					Topics:         []string{testName},
					MaxPollRecords: 4,
				}),
				wkafka.WithLogger(logz.AdapterKV{Log: logger}),
			)
			s.NoError(err)

			err = kafka1.Consume(ctx, wkafka.WithCallback(kafka1Process))
			s.T().Logf("consume error: %v", err)

			kafka1.Close()

			time.Sleep(5 * time.Second)
		}
	})

	errGroup.Go(func() error {
		time.Sleep(10 * time.Second) // Ensure kafka-1 starts first
		// ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		// defer cancel()
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			logger := log.With().Str("consumer", "kafka-2").Logger()
			kafka2, err := wkafka.New(
				ctx, s.container.Config,
				wkafka.WithConsumer(wkafka.ConsumerConfig{
					GroupID: "test-group-rebalance",
					Topics:  []string{testName},
					DLQ: wkafka.DLQConfig{
						ConsumerDisabled: true,
						Topic:            testName + "-dlq",
					},
				}),
				wkafka.WithLogger(logz.AdapterKV{Log: logger}),
			)
			s.NoError(err)

			err = kafka2.Consume(ctx, wkafka.WithCallback(kafka2Process))
			s.T().Logf("consume error: %v", err)

			kafka2.Close()

			time.Sleep(5 * time.Second)
		}
	})

	s.NoError(errGroup.Wait())
}
