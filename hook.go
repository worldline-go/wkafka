package wkafka

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type hooker struct {
	ctx context.Context //nolint:containedctx // replace in consume

	m sync.RWMutex
}

var _ kgo.HookFetchRecordBuffered = new(hooker)

func (h *hooker) OnFetchRecordBuffered(r *kgo.Record) {
	h.m.RLock()
	defer h.m.RUnlock()

	r.Context = h.ctx
}

func (h *hooker) setCtx(ctx context.Context) {
	if h == nil {
		return
	}

	h.m.Lock()
	defer h.m.Unlock()

	h.ctx = ctx
}
