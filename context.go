package wkafka

import "context"

type ctxKey string

const (
	// KeyRecord is the context key for *Record.
	KeyRecord ctxKey = "kafka_record"
)

// GetRecord returns the Record from the context in callback function.
//   - If the context is nil, or the Record is not set, nil is returned.
func GetRecord(ctx context.Context) *Record {
	if ctx == nil {
		return nil
	}

	record, _ := ctx.Value(KeyRecord).(*Record)

	return record
}

func GetRecordBatch(ctx context.Context) []*Record {
	if ctx == nil {
		return nil
	}

	record, _ := ctx.Value(KeyRecord).([]*Record)

	return record
}
