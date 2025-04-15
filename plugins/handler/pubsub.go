package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/worldline-go/conn/connredis"
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

	Redis connredis.Config `cfg:"redis" json:"redis"`
}

func (c *PubSubConfig) New(id string, logger wkafka.Logger) (pubsub, error) {
	if len(c.Redis.Address) > 0 {
		return newRedis(c.Redis, c.Prefix+id, logger)
	}

	return nil, nil
}

func (h *Handler) StartPubSub(ctx context.Context) error {
	h.client.Logger().Info("starting pubsub for handler with topic", "topic", h.pubsub.GetTopic())
	return h.pubsub.Subscribe(ctx, func(data PubSubModel) error {
		switch data.Type {
		case "skip-append":
			var skip wkafka.SkipMap
			if err := json.Unmarshal(data.Value, &skip); err != nil {
				return err
			}

			h.client.Skip(ctx, wkafka.SkipAppend(skip))
		case "skip-replace":
			var skip wkafka.SkipMap
			if err := json.Unmarshal(data.Value, &skip); err != nil {
				return err
			}

			h.client.Skip(ctx, wkafka.SkipReplace(skip))
		case "info":
			var infoID InfoResponseID
			if err := json.Unmarshal(data.Value, &infoID); err != nil {
				return err
			}

			if infoID.ID == "" {
				return fmt.Errorf("info id is empty")
			}

			h.BroadcastInfo(string(data.Value))
		case "delete":
			var infoID InfoResponseID
			if err := json.Unmarshal(data.Value, &infoID); err != nil {
				return err
			}

			if infoID.ID == "" {
				return fmt.Errorf("info id is empty")
			}

			h.BroadcastDelete(infoID.ID)
		case "publish-info":
			// publish info to pubsub
			if err := h.PublishInfo(ctx); err != nil {
				return fmt.Errorf("publish info: %w", err)
			}
		case "retry-dlq":
			var opt wkafka.OptionDLQTrigger
			if err := json.Unmarshal(data.Value, &opt); err != nil {
				return err
			}

			h.client.DLQRetry(opt.ToOption())
		default:
			return fmt.Errorf("unknown type: %s", data.Type)
		}

		return nil
	})
}

func (h *Handler) PublishInfo(ctx context.Context) error {
	info := h.getInfo()

	return h.pubsub.Publish(ctx, PubSubModelPublish{
		Type:  "info",
		Value: info,
	})
}

func (h *Handler) RequestPublishInfo(ctx context.Context) error {
	return h.pubsub.Publish(ctx, PubSubModelPublish{
		Type: "publish-info",
	})
}

func (h *Handler) RequestRetryDLQ(ctx context.Context, o wkafka.OptionDLQTrigger) error {
	return h.pubsub.Publish(ctx, PubSubModelPublish{
		Type:  "retry-dlq",
		Value: o,
	})
}

func (h *Handler) PublishDelete(ctx context.Context) error {
	return h.pubsub.Publish(ctx, PubSubModelPublish{
		Type:  "delete",
		Value: InfoResponseID{ID: h.id},
	})
}

// //////////////////////////////

type Redis struct {
	client redis.UniversalClient
	topic  string
	log    wkafka.Logger
}

type redisLogger struct {
	log wkafka.Logger
}

func (r *redisLogger) Printf(_ context.Context, format string, v ...interface{}) {
	r.log.Warn(fmt.Sprintf(format, v...))
}

func newRedis(r connredis.Config, topic string, logger wkafka.Logger) (*Redis, error) {
	redis.SetLogger(&redisLogger{log: logger})

	client, err := connredis.New(r)
	if err != nil {
		return nil, err
	}

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
			if msg == nil {
				r.log.Error("pubsub message is nil")
				continue
			}

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
