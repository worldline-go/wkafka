// Package redisauth keeps the Redis credentials providers that can be selected
// by name in the handler's Redis config.
//
// Providers register themselves from init, so importing a provider package for
// its side effects is enough to enable it:
//
//	import _ "github.com/worldline-go/wkafka/plugins/handler/redisauth/gcpiam"
package redisauth

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// CredentialsProvider is called for every new connection and returns the
// username and password to use.
type CredentialsProvider func(ctx context.Context) (username, password string, err error)

// Factory builds a CredentialsProvider when a Redis client is created.
type Factory func(ctx context.Context) (CredentialsProvider, error)

var (
	mu        sync.RWMutex
	factories = map[string]Factory{}
)

// Register makes a provider available by name. It panics if the name is
// already taken or factory is nil.
func Register(name string, factory Factory) {
	mu.Lock()
	defer mu.Unlock()

	if factory == nil {
		panic("redisauth: register factory is nil for " + name)
	}
	if _, ok := factories[name]; ok {
		panic("redisauth: register called twice for " + name)
	}

	factories[name] = factory
}

// Get builds the provider registered with name.
func Get(ctx context.Context, name string) (CredentialsProvider, error) {
	mu.RLock()
	factory, ok := factories[name]
	mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("redis credentials provider %q is not registered (forgotten import?), registered: %v", name, names())
	}

	return factory(ctx)
}

func names() []string {
	mu.RLock()
	defer mu.RUnlock()

	list := make([]string, 0, len(factories))
	for name := range factories {
		list = append(list, name)
	}
	sort.Strings(list)

	return list
}
