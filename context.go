package wkafka

import "context"

type ctxKey string

const (
	// KeyRecord is the context key for *Record.
	KeyRecord ctxKey = "kafka_record"
	// KeyIsDLQProcess is the context key for is DLQ.
	KeyIsDLQProcess ctxKey = "kafka_is_dlq"
	// KeyIsDLQEnabled is the context key for is DLQ enabled.
	KeyIsDLQEnabled ctxKey = "kafka_is_dlq_enabled"
)

// CtxIsDLQProcess usable in the callback function to understand processing a DLQ message.
//   - If the context is nil, or the KeyIsDLQProcess is not set, false is returned.
func CtxIsDLQProcess(ctx context.Context) bool {
	if ctx == nil {
		return false
	}

	isDLQ, _ := ctx.Value(KeyIsDLQProcess).(bool)

	return isDLQ
}

// CtxIsDLQEnabled usable in the callback function to understand DLQ is enabled.
//   - If the context is nil, or the KeyIsDLQEnabled is not set, false is returned.
func CtxIsDLQEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}

	isDLQEnabled, _ := ctx.Value(KeyIsDLQEnabled).(bool)

	return isDLQEnabled
}

// CtxRecord returns the *Record from the context in callback function.
//   - If the context is nil, or the Record is not set, nil is returned.
//   - This is only used in callback function if not batch.
//   - Don't edit the returned *Record!
func CtxRecord(ctx context.Context) *Record {
	if ctx == nil {
		return nil
	}

	record, _ := ctx.Value(KeyRecord).(*Record)

	return record
}

// CtxRecordBatch returns the []*Record from the context in callback function.
//   - If the context is nil, or the Record is not set, nil is returned.
//   - This is only used in batch callback function if it is batch.
//   - Don't edit the returned []*Record!
func CtxRecordBatch(ctx context.Context) []*Record {
	if ctx == nil {
		return nil
	}

	records, _ := ctx.Value(KeyRecord).([]*Record)

	return records
}

// CtxRecordWithIndex returns the *Record from the context in callback function.
//   - If the context is nil, or the Record is not set, nil is returned.
//   - This is only used in callback function.
func CtxRecordWithIndex(ctx context.Context, index int) *Record {
	if ctx == nil {
		return nil
	}

	if isDLQ, _ := ctx.Value(KeyIsDLQProcess).(bool); isDLQ {
		record, _ := ctx.Value(KeyRecord).(*Record)

		return record
	}

	records, _ := ctx.Value(KeyRecord).([]*Record)
	if records != nil {
		if index < 0 || index >= len(records) {
			return nil
		}

		return records[index]
	}

	return nil
}
