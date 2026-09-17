package wkafka

// Logger is the logging interface used by wkafka.
//   - *slog.Logger satisfies this interface as is.
type Logger interface {
	Error(msg string, keysAndValues ...any)
	Info(msg string, keysAndValues ...any)
	Debug(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
}

// LogNoop is a Logger that discards all log records.
type LogNoop struct{}

func (LogNoop) Error(_ string, _ ...any) {}
func (LogNoop) Info(_ string, _ ...any)  {}
func (LogNoop) Debug(_ string, _ ...any) {}
func (LogNoop) Warn(_ string, _ ...any)  {}
