package wkafka

type Logger interface {
	Error(msg string, keysAndValues ...interface{})
	Info(msg string, keysAndValues ...interface{})
	Debug(msg string, keysAndValues ...interface{})
	Warn(msg string, keysAndValues ...interface{})
}

type LogNoop struct{}

func (LogNoop) Error(_ string, _ ...interface{}) {}
func (LogNoop) Info(_ string, _ ...interface{})  {}
func (LogNoop) Debug(_ string, _ ...interface{}) {}
func (LogNoop) Warn(_ string, _ ...interface{})  {}
