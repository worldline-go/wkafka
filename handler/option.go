package handler

type option struct {
	PathPrefix string
	Addr       string
	PubSub     *PubSubConfig
}

func (o *option) apply(opts ...Option) {
	for _, opt := range opts {
		opt(o)
	}
}

type Option func(*option)

// WithPathPrefix to set prefix path for handler.
func WithPathPrefix(prefix string) Option {
	return func(o *option) {
		o.PathPrefix = prefix
	}
}

// WithAddr to set address for handler.
//   - Only for Serve method, default is ":17070".
func WithAddr(addr string) Option {
	return func(o *option) {
		o.Addr = addr
	}
}

// WithPubSub to set pubsub configuration.
func WithPubSub(cfg *PubSubConfig) Option {
	return func(o *option) {
		o.PubSub = cfg
	}
}
