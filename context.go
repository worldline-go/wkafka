package wkafka

import "context"

type ctxKey string

const (
	// KeyRecord is the context key for *Record.
	KeyRecord ctxKey = "kafka_record"
	// KeyIsDLQ is the context key for is DLQ.
	KeyIsDLQ ctxKey = "kafka_is_dlq"
)

// CtxIsDLQ usable in the callback function to understand processing a DLQ message.
func CtxIsDLQ(ctx context.Context) bool {
	if ctx == nil {
		return false
	}

	isDLQ, _ := ctx.Value(KeyIsDLQ).(bool)

	return isDLQ
}

// CtxRecord returns the *Record from the context in callback function.
//   - If the context is nil, or the Record is not set, nil is returned.
//   - This is only used in callback function.
func CtxRecord(ctx context.Context) *Record {
	if ctx == nil {
		return nil
	}

	record, _ := ctx.Value(KeyRecord).(*Record)

	return record
}

// CtxRecordBatch returns the []*Record from the context in callback function.
//   - If the context is nil, or the Record is not set, nil is returned.
//   - This is only used in batch callback function.
func CtxRecordBatch(ctx context.Context) []*Record {
	if ctx == nil {
		return nil
	}

	record, _ := ctx.Value(KeyRecord).([]*Record)

	return record
}
