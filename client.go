package wkafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

type Client struct {
	Kafka    *kgo.Client
	KafkaDLQ *kgo.Client

	clientID       []byte
	consumerConfig ConsumerConfig
}

func New(ctx context.Context, cfg Config, opts ...Option) (*Client, error) {
	o := options{
		ClientID:          DefaultClientID,
		AutoTopicCreation: true,
		AppName:           idProgname,
	}

	o.apply(opts...)

	// validate client and add defaults to consumer config
	consumerConfig, err := cfg.Consumer.Apply(o.ConsumerConfig, o.AppName)
	if err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	o.ConsumerConfig = consumerConfig

	kgoClient, err := newClient(ctx, cfg, o)
	if err != nil {
		return nil, err
	}

	var kgoClientDLQ *kgo.Client
	if o.ConsumerEnabled {
		kgoClientDLQ, err = newClient(ctx, cfg, o.WithDLQ())
		if err != nil {
			return nil, err
		}
	}

	cl := &Client{
		Kafka:          kgoClient,
		KafkaDLQ:       kgoClientDLQ,
		clientID:       []byte(o.ClientID),
		consumerConfig: o.ConsumerConfig,
	}

	// main and dlq use same config, ask for validation once
	if err := cl.Kafka.Ping(ctx); err != nil {
		return nil, fmt.Errorf("connection to kafka brokers: %w", err)
	}

	return cl, nil
}

func newClient(ctx context.Context, cfg Config, o options) (*kgo.Client, error) {
	compressions, err := compressionOpts(cfg.Compressions)
	if err != nil {
		return nil, err
	}

	kgoOpt := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(o.ClientID),
		kgo.ProducerBatchCompression(compressions...),
		// kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)),
	}

	// Auto topic creation
	if o.AutoTopicCreation {
		kgoOpt = append(kgoOpt, kgo.AllowAutoTopicCreation())
	}

	// TLS authentication
	tlsConfig, err := cfg.Security.TLS.Generate()
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		kgoOpt = append(kgoOpt, kgo.DialTLSConfig(tlsConfig))
	}

	// SASL authentication
	saslOpts, err := cfg.Security.SASL.Generate()
	if err != nil {
		return nil, err
	}
	if len(saslOpts) > 0 {
		kgoOpt = append(kgoOpt, kgo.SASL(saslOpts...))
	}

	if o.ConsumerEnabled {
		// validate consumer
		if err := cfg.Consumer.Validation.Validate(o.ConsumerConfig); err != nil {
			return nil, fmt.Errorf("validate consumer config: %w", err)
		}

		var startOffsetCfg int64
		if o.DLQ {
			startOffsetCfg = o.ConsumerConfig.DLQ.StartOffset
		} else {
			startOffsetCfg = o.ConsumerConfig.StartOffset
		}

		// start offset settings
		startOffset := kgo.NewOffset()
		switch v := startOffsetCfg; {
		case v == 0 || v == -2 || v < -2:
			startOffset = startOffset.AtStart()
		case v == -1:
			startOffset = startOffset.AtEnd()
		default:
			startOffset = startOffset.At(startOffsetCfg)
		}

		kgoOpt = append(kgoOpt,
			kgo.DisableAutoCommit(),
			kgo.RequireStableFetchOffsets(),
			kgo.ConsumerGroup(o.ConsumerConfig.GroupID),
			kgo.ConsumeResetOffset(startOffset),
		)

		if o.DLQ {
			kgoOpt = append(kgoOpt, kgo.ConsumeTopics(o.ConsumerConfig.DLQ.Topic))
		} else {
			kgoOpt = append(kgoOpt, kgo.ConsumeTopics(o.ConsumerConfig.Topics...))
		}
	}

	// Add custom options
	if o.DLQ {
		kgoOpt = append(kgoOpt, o.KGOOptionsDLQ...)
	} else {
		kgoOpt = append(kgoOpt, o.KGOOptions...)
	}

	// Create kafka client
	kgoClient, err := kgo.NewClient(kgoOpt...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	return kgoClient, nil
}

func (c *Client) Close() {
	if c.Kafka != nil {
		c.Kafka.Close()
	}
}

// Consume starts consuming messages from kafka.
//   - Only works if client is created with consumer config.
func (c *Client) Consume(ctx context.Context, callback CallBackFunc, opts ...OptionConsumer) error {
	o := optionConsumer{
		Client:         c,
		ConsumerConfig: c.consumerConfig,
	}

	opts = append([]OptionConsumer{OptionConsumer(callback)}, opts...)

	if err := o.apply(opts...); err != nil {
		return err
	}

	if o.Consumer == nil {
		return fmt.Errorf("consumer is nil: %w", ErrNotImplemented)
	}

	if o.ConsumerDLQ == nil {
		if err := o.Consumer.Consume(ctx, c.Kafka); err != nil {
			return fmt.Errorf("failed to consume: %w", err)
		}

		return nil
	}

	// consume main and dlq concurrently
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := o.Consumer.Consume(ctx, c.Kafka); err != nil {
			return fmt.Errorf("failed to consume: %w", err)
		}

		return nil
	})

	g.Go(func() error {
		if err := o.ConsumerDLQ.Consume(ctx, c.KafkaDLQ); err != nil {
			return fmt.Errorf("failed to consume DLQ: %w", err)
		}

		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

// Produce sends a message to kafka. For type producer check wkafka.NewProducer.
func (c *Client) ProduceRaw(ctx context.Context, records []*kgo.Record) error {
	result := c.Kafka.ProduceSync(ctx, records...)

	return result.FirstErr()
}

// Admin returns an admin client to manage kafka.
func (c *Client) Admin() *kadm.Client {
	return kadm.NewClient(c.Kafka)
}
