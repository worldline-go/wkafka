package wkafka

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/worldline-go/logz"
	"golang.org/x/sync/errgroup"
)

type Client struct {
	Kafka               *kgo.Client
	KafkaDLQ            *kgo.Client
	partitionHandler    *partitionHandler
	partitionHandlerDLQ *partitionHandler

	clientID       []byte
	consumerConfig *ConsumerConfig
	logger         logz.Adapter

	// log purpose

	dlqTopics []string
	topics    []string
}

func New(ctx context.Context, cfg Config, opts ...Option) (*Client, error) {
	o := options{
		ClientID:          DefaultClientID,
		AutoTopicCreation: true,
		AppName:           idProgname,
		Logger:            logz.AdapterKV{Log: log.Logger},
	}

	o.apply(opts...)

	// validate client and add defaults to consumer config
	if o.ConsumerConfig != nil {
		if err := configApply(cfg.Consumer, o.ConsumerConfig, o.AppName, o.Logger); err != nil {
			return nil, fmt.Errorf("validate config: %w", err)
		}

		if !o.ConsumerConfig.DLQ.Disable {
			o.ConsumerDLQEnabled = true
		}
	}

	c := &Client{
		consumerConfig: o.ConsumerConfig,
		logger:         o.Logger,
		clientID:       []byte(o.ClientID),
	}

	kgoClient, err := newClient(c, cfg, &o, false)
	if err != nil {
		return nil, err
	}

	var kgoClientDLQ *kgo.Client
	if o.ConsumerDLQEnabled {
		kgoClientDLQ, err = newClient(c, cfg, &o, true)
		if err != nil {
			return nil, err
		}
	}

	c.Kafka = kgoClient
	c.KafkaDLQ = kgoClientDLQ

	// main and dlq use same config, ask for validation once
	if err := c.Kafka.Ping(ctx); err != nil {
		return nil, fmt.Errorf("connection to kafka brokers: %w", err)
	}

	return c, nil
}

func newClient(c *Client, cfg Config, o *options, isDLQ bool) (*kgo.Client, error) {
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
		var startOffsetCfg int64
		if isDLQ {
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

		// create partition handler
		var partitionH *partitionHandler
		if isDLQ {
			c.partitionHandlerDLQ = &partitionHandler{
				logger: c.logger,
			}
			partitionH = c.partitionHandlerDLQ
		} else {
			c.partitionHandler = &partitionHandler{
				logger: c.logger,
			}
			partitionH = c.partitionHandler
		}

		kgoOpt = append(kgoOpt,
			kgo.DisableAutoCommit(),
			kgo.RequireStableFetchOffsets(),
			kgo.ConsumerGroup(o.ConsumerConfig.GroupID),
			kgo.ConsumeResetOffset(startOffset),
			kgo.OnPartitionsLost(partitionLost(partitionH)),
			kgo.OnPartitionsRevoked(partitionRevoked(partitionH)),
		)

		if isDLQ {
			topics := []string{o.ConsumerConfig.DLQ.Topic}
			if len(o.ConsumerConfig.DLQ.TopicsExtra) > 0 {
				topics = append(topics, o.ConsumerConfig.DLQ.TopicsExtra...)
			}

			kgoOpt = append(kgoOpt, kgo.ConsumeTopics(topics...))
			c.dlqTopics = topics
		} else {
			kgoOpt = append(kgoOpt, kgo.ConsumeTopics(o.ConsumerConfig.Topics...))
			c.topics = o.ConsumerConfig.Topics
		}
	}

	// add custom options
	if isDLQ {
		kgoOpt = append(kgoOpt, o.KGOOptionsDLQ...)
	} else {
		kgoOpt = append(kgoOpt, o.KGOOptions...)
	}

	// create kafka client
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
	if c.KafkaDLQ != nil {
		c.KafkaDLQ.Close()
	}
}

// Consume starts consuming messages from kafka and blocks until context is done or an error occurs.
//   - Only works if client is created with consumer config.
//   - Just run one time.
func (c *Client) Consume(ctx context.Context, callback CallBackFunc, opts ...OptionConsumer) error {
	o := optionConsumer{
		Client:         c,
		ConsumerConfig: *c.consumerConfig,
	}

	opts = append([]OptionConsumer{OptionConsumer(callback)}, opts...)

	if err := o.apply(opts...); err != nil {
		return err
	}

	if o.Consumer == nil {
		return fmt.Errorf("consumer is nil: %w", errNotImplemented)
	}

	// consume main only
	if c.KafkaDLQ == nil {
		c.logger.Info("wkafka start consuming", "topics", c.topics)
		if err := o.Consumer.Consume(ctx, c.Kafka); err != nil {
			return fmt.Errorf("failed to consume: %w", err)
		}

		return nil
	}

	// consume main and dlq concurrently
	g, ctx := errgroup.WithContext(ctx)

	ctx = context.WithValue(ctx, KeyIsDLQEnabled, true)

	g.Go(func() error {
		c.logger.Info("wkafka start consuming", "topics", c.topics)
		if err := o.Consumer.Consume(ctx, c.Kafka); err != nil {
			return fmt.Errorf("failed to consume: %w", err)
		}

		return nil
	})

	g.Go(func() error {
		c.logger.Info("wkafka start consuming DLQ", "topics", c.dlqTopics)
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
