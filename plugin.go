package wkafka

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"sync"
	"time"

	"github.com/worldline-go/struct2"
)

type PluginFunc[T any] func(ctx context.Context, client *Client, cfg T) error

type pluginWrapFunc func(ctx context.Context, client *Client, cfg any) error

type plugin struct {
	holder map[string]pluginWrapFunc

	m sync.Mutex
}

func pluginConvert[T any](fn PluginFunc[T]) pluginWrapFunc {
	return func(ctx context.Context, client *Client, config any) error {
		var cfg T

		decoder := struct2.Decoder{
			TagName:               "cfg",
			WeaklyTypedInput:      true,
			WeaklyDashUnderscore:  true,
			WeaklyIgnoreSeperator: true,
			HooksDecode: []struct2.HookDecodeFunc{
				hookTimeDuration,
			},
		}
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

var registeredPlugins plugin

// RegisterPlugin makes a plugin run for every client, usually called from the
// plugin package's init so importing it is enough. The plugin is called even
// when its config is missing, so it should do nothing unless enabled.
//
// A plugin given with WithPlugin under the same name replaces the registered one.
func RegisterPlugin[T any](name string, fn PluginFunc[T]) {
	registeredPlugins.Add(name, pluginConvert(fn))
}

// plugins returns the registered plugins merged with the client's own.
func (p *plugin) plugins() map[string]pluginWrapFunc {
	registeredPlugins.m.Lock()
	merged := maps.Clone(registeredPlugins.holder)
	registeredPlugins.m.Unlock()

	if merged == nil {
		merged = make(map[string]pluginWrapFunc)
	}

	p.m.Lock()
	maps.Copy(merged, p.holder)
	p.m.Unlock()

	return merged
}

// //////////////////
// Hooks for decoder plugin

// hookTimeDuration for time.Duration
func hookTimeDuration(in, out reflect.Type, data any) (any, error) {
	if out == reflect.TypeFor[time.Duration]() {
		switch in.Kind() {
		case reflect.String:
			return time.ParseDuration(data.(string))
		case reflect.Int:
			return time.Duration(data.(int)), nil
		case reflect.Int64:
			return time.Duration(data.(int64)), nil
		case reflect.Float64:
			return time.Duration(data.(float64)), nil
		}
	}

	return data, nil
}
