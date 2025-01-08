package wkafka

import (
	"context"
	"sync"
)

type PluginFunc func(ctx context.Context, client *Client, cfg interface{}) error

type plugin struct {
	holder map[string]PluginFunc

	m sync.Mutex
}

func (p *plugin) Add(name string, fn PluginFunc) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.holder == nil {
		p.holder = make(map[string]PluginFunc)
	}

	p.holder[name] = fn
}
