package handler

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"

	"github.com/worldline-go/wkafka"
	wkafkahandler "github.com/worldline-go/wkafka/handler/gen/wkafka"
	"github.com/worldline-go/wkafka/handler/gen/wkafka/wkafkaconnect"
)

type Handler struct {
	Client *wkafka.Client
}

// NewHandler returns a http.Handler implementation.
func New(client *wkafka.Client) (string, http.Handler) {
	return wkafkaconnect.NewWkafkaServiceHandler(&Handler{
		Client: client,
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
	slog.Debug("topics", slog.Any("topics", topics))

	for k, v := range topics {
		slog.Debug("skip", slog.String("topic", k), slog.Any("partitions", v.GetPartitions()))
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
