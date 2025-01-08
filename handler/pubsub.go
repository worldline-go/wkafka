package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/worldline-go/wkafka"
)

type pubsub interface {
	Subscribe(ctx context.Context, handler func(PubSubModel) error) error
	Publish(ctx context.Context, data PubSubModelPublish) error
	Close() error
	GetTopic() string
}

type PubSubConfig struct {
	Prefix string `cfg:"prefix" json:"prefix"`

	Redis *RedisConfig `cfg:"redis" json:"redis"`
}

func (c *PubSubConfig) New(groupID string, logger wkafka.Logger) (pubsub, error) {
	if c.Redis != nil {
		return newRedis(c.Redis, c.Prefix+groupID, logger)
	}

	return nil, nil
}

func (h *Handler) StartPubSub(ctx context.Context) error {
	h.client.GetLogger().Info("starting pubsub for handler with topic", "topic", h.pubsub.GetTopic())
	return h.pubsub.Subscribe(ctx, func(data PubSubModel) error {
		switch data.Type {
		case "skip-append":
			var skip wkafka.SkipMap
			if err := json.Unmarshal(data.Value, &skip); err != nil {
				return err
			}

			h.client.Skip(wkafka.SkipAppend(skip))
		case "skip-replace":
			var skip wkafka.SkipMap
			if err := json.Unmarshal(data.Value, &skip); err != nil {
				return err
			}

			h.client.Skip(wkafka.SkipReplace(skip))
		default:
			return fmt.Errorf("unknown type: %s", data.Type)
		}

		return nil
	})
}

// //////////////////////////////

type RedisConfig struct {
	Address  string           `cfg:"address"  json:"address"`
	Username string           `cfg:"username" json:"username"`
	Password string           `cfg:"password" json:"password"`
	TLS      wkafka.TLSConfig `cfg:"tls"      json:"tls"`
}

type Redis struct {
	client *redis.Client
	topic  string
	log    wkafka.Logger
}

type redisLogger struct {
	log wkafka.Logger
}

func (r *redisLogger) Printf(_ context.Context, format string, v ...interface{}) {
	r.log.Warn(fmt.Sprintf(format, v...))
}

func newRedis(r *RedisConfig, topic string, logger wkafka.Logger) (*Redis, error) {
	tlsConfig, err := r.TLS.Generate()
	if err != nil {
		return nil, err
	}

	redis.SetLogger(&redisLogger{log: logger})

	client := redis.NewClient(&redis.Options{
		Addr:      r.Address,
		Username:  r.Username,
		Password:  r.Password,
		TLSConfig: tlsConfig,
	})

	return &Redis{
		client: client,
		topic:  topic,
		log:    logger,
	}, nil
}

func (r *Redis) Close() error {
	return r.client.Close()
}

func (r *Redis) Publish(ctx context.Context, data PubSubModelPublish) error {
	if data.Type == "" {
		return fmt.Errorf("type is empty")
	}

	v, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return r.client.Publish(ctx, r.topic, v).Err()
}

func (r *Redis) Subscribe(ctx context.Context, handler func(PubSubModel) error) error {
	pubsub := r.client.Subscribe(ctx, r.topic)
	defer pubsub.Close()

	ch := pubsub.Channel()
	for {
		select {
		case msg := <-ch:
			var data PubSubModel
			if err := json.Unmarshal([]byte(msg.Payload), &data); err != nil {
				r.log.Error("pubsub unmarshal", "error", err)
				continue
			}

			if err := handler(data); err != nil {
				r.log.Error("pubsub handler", "error", err)
				continue
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *Redis) GetTopic() string {
	return r.topic
}
