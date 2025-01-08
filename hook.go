package wkafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type hooker struct {
	ctx context.Context //nolint:containedctx // replace in consume
}

var _ kgo.HookFetchRecordBuffered = new(hooker)

func (h *hooker) OnFetchRecordBuffered(r *kgo.Record) {
	r.Context = h.ctx
}

func (h *hooker) setCtx(ctx context.Context) {
	if h == nil {
		return
	}

	h.ctx = ctx
}
