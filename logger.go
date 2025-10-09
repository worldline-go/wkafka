package wkafka

type Logger interface {
	Error(msg string, keysAndValues ...any)
	Info(msg string, keysAndValues ...any)
	Debug(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
}

type LogNoop struct{}

func (LogNoop) Error(_ string, _ ...any) {}
func (LogNoop) Info(_ string, _ ...any)  {}
func (LogNoop) Debug(_ string, _ ...any) {}
func (LogNoop) Warn(_ string, _ ...any)  {}
