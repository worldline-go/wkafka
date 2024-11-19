package handler

type option struct {
	PathPrefix string
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
