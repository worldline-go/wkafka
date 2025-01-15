package wkafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/worldline-go/struct2"
)

type PluginFunc[T any] func(ctx context.Context, client *Client, cfg T) error

type pluginWrapFunc func(ctx context.Context, client *Client, cfg interface{}) error

type plugin struct {
	holder map[string]pluginWrapFunc

	m sync.Mutex
}

func pluginConvert[T any](fn PluginFunc[T]) pluginWrapFunc {
	return func(ctx context.Context, client *Client, config interface{}) error {
		var cfg T

		decoder := struct2.Decoder{TagName: "cfg"}
		if err := decoder.Decode(config, &cfg); err != nil {
			return fmt.Errorf("failed to decode config: %w", err)
		}

		return fn(ctx, client, cfg)
	}
}

func (p *plugin) Add(name string, fn pluginWrapFunc) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.holder == nil {
		p.holder = make(map[string]pluginWrapFunc)
	}

	p.holder[name] = fn
}
