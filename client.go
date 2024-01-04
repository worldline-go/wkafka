package wkafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Client struct {
	Kafka *kgo.Client

	clientID       []byte
	consumerConfig ConsumerConfig
}

func New(ctx context.Context, cfg Config, opts ...Option) (*Client, error) {
	o := options{
		ClientID:          DefaultClientID,
		AutoTopicCreation: true,
	}
	for _, opt := range opts {
		opt(&o)
	}

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

		// start offset settings
		startOffset := kgo.NewOffset()
		switch v := o.ConsumerConfig.StartOffset; {
		case v == 0 || v == -2 || v < -2:
			startOffset = startOffset.AtStart()
		case v == -1:
			startOffset = startOffset.AtEnd()
		default:
			startOffset = startOffset.At(o.ConsumerConfig.StartOffset)
		}

		kgoOpt = append(kgoOpt,
			kgo.DisableAutoCommit(),
			kgo.RequireStableFetchOffsets(),
			kgo.ConsumerGroup(o.ConsumerConfig.GroupID),
			kgo.ConsumeTopics(o.ConsumerConfig.Topics...),
			kgo.ConsumeResetOffset(startOffset),
		)
	}

	// Add custom options
	kgoOpt = append(kgoOpt, o.KGOOptions...)

	// Create kafka client
	kgoClient, err := kgo.NewClient(kgoOpt...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	cl := &Client{
		Kafka:          kgoClient,
		clientID:       []byte(o.ClientID),
		consumerConfig: o.ConsumerConfig,
	}

	if err := cl.Kafka.Ping(ctx); err != nil {
		return nil, fmt.Errorf("connection to kafka brokers: %w", err)
	}

	return cl, nil
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

	if err := o.Consumer.Consume(ctx, c.Kafka); err != nil {
		return fmt.Errorf("failed to consume: %w", err)
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
