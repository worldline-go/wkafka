package wkafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/worldline-go/logz"
	"golang.org/x/sync/errgroup"
)

var ErrConnection = errors.New("connect to kafka brokers failed")

type Client struct {
	Kafka               *kgo.Client
	KafkaDLQ            *kgo.Client
	partitionHandler    *partitionHandler
	partitionHandlerDLQ *partitionHandler

	clientID       []byte
	consumerConfig *ConsumerConfig
	consumerMutex  sync.RWMutex
	logger         Logger

	dlqRecord       DLQRecord
	dlqRetryTrigger func(opts []OptionDLQTriggerFn)
	dlqMutex        sync.RWMutex

	hook    *hooker
	cancel  context.CancelFunc
	trigger []func(context.Context)

	topicsCheck []string
	appName     string
	// log purpose

	brokers   []string
	dlqTopics []string
	topics    []string
	meter     Meter
}

func New(ctx context.Context, cfg Config, opts ...Option) (*Client, error) {
	o := options{
		ClientID:          DefaultClientID,
		AutoTopicCreation: true,
		AppName:           idProgname,
		Logger:            logz.AdapterKV{Log: log.Logger},
		Ping:              true,
		PingRetry:         false,
	}

	o.apply(opts...)

	if o.Logger == nil {
		o.Logger = LogNoop{}
	}

	// validate client and add defaults to consumer config
	if o.ConsumerConfig != nil {
		if err := configApply(cfg.Consumer, o.ConsumerConfig, o.AppName, o.Logger); err != nil {
			return nil, fmt.Errorf("validate config: %w", err)
		}

		if !o.ConsumerConfig.DLQ.ConsumerDisabled {
			o.ConsumerDLQEnabled = true
		}
	}

	if o.Meter == nil {
		o.Meter = noopMeter()
	}

	c := &Client{
		consumerConfig: o.ConsumerConfig,
		logger:         o.Logger,
		clientID:       []byte(o.ClientID),
		meter:          o.Meter,
		hook: &hooker{
			ctx: context.Background(),
		},
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

	if o.Ping {
		if o.PingRetry {
			if o.PingBackoff == nil {
				o.PingBackoff = defaultBackoff()
			}

			b := backoff.WithContext(o.PingBackoff, ctx)

			if err := backoff.RetryNotify(func() error {
				if err := c.Kafka.Ping(ctx); err != nil {
					return fmt.Errorf("%w: %w", ErrConnection, err)
				}

				return nil
			}, b, func(err error, d time.Duration) {
				c.logger.Warn("wkafka ping failed", "error", err.Error(), "retry_in", d.String())
			}); err != nil {
				return nil, err
			}
		} else {
			// main and dlq use same config, ask for validation once
			if err := c.Kafka.Ping(ctx); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrConnection, err)
			}
		}
	}

	c.brokers = cfg.Brokers

	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	c.topicsCheck = slices.Concat(c.topics, c.dlqTopics)
	c.appName = o.AppName

	for name, p := range o.Plugin.holder {
		if err := p(ctx, c, cfg.Plugins[name]); err != nil {
			return nil, fmt.Errorf("plugin %s: %w", name, err)
		}
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
			kgo.OnPartitionsLost(partitionLost(partitionH, c.DLQRetry)),
			kgo.OnPartitionsRevoked(partitionRevoked(partitionH, c.DLQRetry)),
			kgo.OnPartitionsAssigned(partitionsAssigned(partitionH)),
			kgo.WithHooks(c.hook),
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
		if len(o.KGOOptionsDLQ) > 0 {
			kgoOpt = append(kgoOpt, o.KGOOptionsDLQ...)
		} else {
			kgoOpt = append(kgoOpt, o.KGOOptions...)
		}
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

	// cancel for plugins
	c.cancel()
}

// GroupID returns the consumer group id.
func (c *Client) GroupID() string {
	if c.consumerConfig == nil {
		return ""
	}

	return c.consumerConfig.GroupID
}

// Consume starts consuming messages from kafka and blocks until context is done or an error occurs.
//   - Only works if client is created with consumer config.
//   - Just run one time.
func (c *Client) Consume(ctx context.Context, callback CallBackFunc, opts ...OptionConsumer) error {
	o := optionConsumer{
		Client:         c,
		ConsumerConfig: c.consumerConfig,
		Meter:          c.meter,
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
		c.hook.setCtx(ctx)

		c.logger.Info("wkafka start consuming", "topics", c.topics)
		if err := o.Consumer.Consume(ctx, c.Kafka); err != nil {
			return fmt.Errorf("failed to consume %v: %w", c.topics, err)
		}

		return nil
	}

	// consume main and dlq concurrently
	g, ctx := errgroup.WithContext(ctx)

	ctx = context.WithValue(ctx, KeyIsDLQEnabled, true)

	c.hook.setCtx(ctx)

	g.Go(func() error {
		c.logger.Info("wkafka start consuming", "topics", c.topics)
		if err := o.Consumer.Consume(ctx, c.Kafka); err != nil {
			return fmt.Errorf("failed to consume %v: %w", c.topics, err)
		}

		return nil
	})

	g.Go(func() error {
		c.logger.Info("wkafka start consuming DLQ", "topics", c.dlqTopics)
		if err := o.ConsumerDLQ.Consume(ctx, c.KafkaDLQ); err != nil {
			return fmt.Errorf("failed to consume DLQ %v: %w", c.topics, err)
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

	if err := result.FirstErr(); err != nil {
		return errors.Join(ctx.Err(), err)
	}

	return nil
}

// Admin returns an admin client to manage kafka.
func (c *Client) Admin() *kadm.Client {
	return kadm.NewClient(c.Kafka)
}

func (c *Client) modifySkip(modify func(SkipMap) SkipMap) {
	c.consumerMutex.Lock()
	defer c.consumerMutex.Unlock()

	newSkip := modify(cloneSkip(c.consumerConfig.Skip))

	// eliminate not related topics
	for topic := range newSkip {
		if !slices.Contains(c.topicsCheck, topic) {
			delete(newSkip, topic)
		}
	}

	c.consumerConfig.Skip = newSkip
}

// Skip for modifying skip configuration in runtime.
//   - Useful for DLQ topic.
//   - Don't wait inside the modify function.
func (c *Client) Skip(ctx context.Context, modify func(SkipMap) SkipMap) {
	if modify == nil {
		return
	}

	c.modifySkip(modify)

	c.callTrigger(ctx)
	if c.dlqRetryTrigger != nil {
		c.dlqRetryTrigger(nil)
	}

	c.logger.Info("wkafka skip modified", "skip", c.consumerConfig.Skip)
}

// SkipCheck returns skip configuration's deep clone.
func (c *Client) SkipCheck() SkipMap {
	c.consumerMutex.RLock()
	defer c.consumerMutex.RUnlock()

	return cloneSkip(c.consumerConfig.Skip)
}

func (c *Client) ClientID() []byte {
	return c.clientID
}

// setDLQRecord to set stucked DLQRecord.
//   - Using in DLQ iteration.
func (c *Client) setDLQRecord(r *kgo.Record, t time.Time, err error) {
	c.dlqMutex.Lock()
	defer c.dlqMutex.Unlock()

	c.dlqRecord.Err = err
	c.dlqRecord.Record = r
	c.dlqRecord.RetryAt = t
}

// DLQRecord returns stucked DLQRecord if exists.
//   - Warning: return pointer record and not modify it.
func (c *Client) DLQRecord() DLQRecord {
	c.dlqMutex.RLock()
	defer c.dlqMutex.RUnlock()

	return c.dlqRecord
}

// DLQRetry to trigger DLQ retry and not wait sleep.
func (c *Client) DLQRetry(opts ...OptionDLQTriggerFn) {
	if c.dlqRetryTrigger != nil {
		c.dlqRetryTrigger(opts)
	}
}

func (c *Client) callTrigger(ctx context.Context) {
	go func() {
		for _, t := range c.trigger {
			if ctx.Err() != nil {
				return
			}

			t(ctx)
		}
	}()
}

func (c *Client) AddTrigger(t func(context.Context)) {
	c.trigger = append(c.trigger, t)
}

func (c *Client) Logger() Logger {
	return c.logger
}

func (c *Client) Topics() []string {
	return c.topics
}

func (c *Client) Brokers() []string {
	return c.brokers
}

func (c *Client) DLQTopics() []string {
	return c.dlqTopics
}

func (c *Client) AppName() string {
	return c.appName
}

func (c *Client) Ping() error {
	if c.Kafka == nil {
		return errors.New("kafka client is nil")
	}

	if err := c.Kafka.Ping(c.hook.ctx); err != nil {
		return err
	}

	return nil
}
