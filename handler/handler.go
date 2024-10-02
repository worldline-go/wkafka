package handler

import (
	"context"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/emptypb"

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

func (h *Handler) Info(ctx context.Context, req *connect.Request[emptypb.Empty]) (*connect.Response[wkafkahandler.InfoResponse], error) {
	var skipList map[string]*wkafkahandler.Topic
	h.Client.Skip(func(m wkafka.SkipMap) wkafka.SkipMap {
		skipList = make(map[string]*wkafkahandler.Topic, len(m))
		for k, v := range m {
			p := make(map[int32]*wkafkahandler.Partition, len(v))
			for k, v := range v {
				p[k] = &wkafkahandler.Partition{
					Before:  v.Before,
					Offsets: v.Offsets,
				}
			}

			skipList[k] = &wkafkahandler.Topic{
				Partitions: p,
			}
		}

		return m
	})

	return connect.NewResponse(&wkafkahandler.InfoResponse{
		Skip: skipList,
	}), nil
}

func (h *Handler) Skip(ctx context.Context, req *connect.Request[wkafkahandler.CreateSkipRequest]) (*connect.Response[wkafkahandler.Response], error) {
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
