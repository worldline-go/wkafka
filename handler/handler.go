package handler

import (
	"context"
	"fmt"
	"net/http"
	"slices"

	"connectrpc.com/connect"

	"github.com/worldline-go/wkafka"
	wkafkahandler "github.com/worldline-go/wkafka/handler/gen/wkafka"
	"github.com/worldline-go/wkafka/handler/gen/wkafka/wkafkaconnect"
)

type Handler struct {
	Client *wkafka.Client

	Logger wkafka.Logger
}

type option struct {
	Logger wkafka.Logger
}

func (o *option) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}

	if o.Logger == nil {
		o.Logger = wkafka.LogNoop{}
	}
}

type Option func(*option)

func WithLogger(logger wkafka.Logger) Option {
	return func(o *option) {
		o.Logger = logger
	}
}

// NewHandler returns a http.Handler implementation.
func New(client *wkafka.Client, opts ...Option) (string, http.Handler) {
	o := option{}
	o.apply(opts...)

	return wkafkaconnect.NewWkafkaServiceHandler(&Handler{
		Client: client,
		Logger: o.Logger,
	})
}

func convertSkipMap(skip map[string]*wkafkahandler.Topic) wkafka.SkipMap {
	if len(skip) == 0 {
		return nil
	}

	m := make(wkafka.SkipMap, len(skip))
	for k, v := range skip {
		p := make(map[int32]wkafka.OffsetConfig, len(v.GetPartitions()))
		for k, v := range v.GetPartitions() {
			p[k] = wkafka.OffsetConfig{
				Before:  v.GetBefore(),
				Offsets: v.GetOffsets(),
			}
		}

		m[k] = p
	}

	return m
}

func (h *Handler) Skip(ctx context.Context, req *connect.Request[wkafkahandler.CreateSkipRequest]) (*connect.Response[wkafkahandler.Response], error) {
	topics := req.Msg.GetTopics()
	h.Logger.Debug("skip topics", "topics", topics)

	if !req.Msg.GetEnableMainTopics() {
		dlqTopics := h.Client.DLQTopics()

		deleteTopicsInList := make([]string, 0)
		for k := range topics {
			if !slices.Contains(dlqTopics, k) {
				deleteTopicsInList = append(deleteTopicsInList, k)
			}
		}

		for _, k := range deleteTopicsInList {
			delete(topics, k)
		}
	}

	switch req.Msg.GetOption() {
	case wkafkahandler.SkipOption_APPEND:
		h.Client.Skip(wkafka.SkipAppend(convertSkipMap(req.Msg.GetTopics())))

		return connect.NewResponse(&wkafkahandler.Response{
			Message: "skip appended",
		}), nil
	case wkafkahandler.SkipOption_REPLACE:
		h.Client.Skip(wkafka.SkipReplace(convertSkipMap(req.Msg.GetTopics())))

		return connect.NewResponse(&wkafkahandler.Response{
			Message: "skip replaced",
		}), nil
	}

	return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid option: %v", req.Msg.GetOption()))
}
