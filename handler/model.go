package handler

import (
	"time"

	"github.com/worldline-go/wkafka"
)

type SkipRequest struct {
	Skip map[string]map[int32]wkafka.OffsetConfig `json:"skip"`
}

type Response struct {
	Message string `json:"message"`
}

type InfoResponse struct {
	DLQTopics []string       `json:"dlq_topics"`
	Topics    []string       `json:"topics"`
	Skip      wkafka.SkipMap `json:"skip"`
	DLQRecord *DLQRecord     `json:"dlq_record"`
}

type DLQRecord struct {
	Topic     string   `json:"topic"`
	Partition int32    `json:"partition"`
	Offset    int64    `json:"offset"`
	Key       []byte   `json:"key"`
	Value     []byte   `json:"value"`
	Timestamp string   `json:"timestamp"`
	Headers   []Header `json:"headers"`
}

type Header struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func dlqRecord(r *wkafka.Record) *DLQRecord {
	if r == nil {
		return nil
	}

	var headers []Header
	if r.Headers != nil {
		headers = make([]Header, 0, len(r.Headers))
		for _, h := range r.Headers {
			headers = append(headers, Header{
				Key:   h.Key,
				Value: h.Value,
			})
		}
	}

	return &DLQRecord{
		Topic:     r.Topic,
		Partition: r.Partition,
		Offset:    r.Offset,
		Key:       r.Key,
		Value:     r.Value,
		Timestamp: r.Timestamp.Format(time.RFC3339),
		Headers:   headers,
	}
}
