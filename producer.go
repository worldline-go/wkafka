package wkafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type (
	Header = kgo.RecordHeader
	Record = kgo.Record
)

type Producer[T any] interface {
	Produce(ctx context.Context, data ...T) error
}

type ProducerHook interface {
	ProduceHook(r *Record)
}

type ProducerConfig[T any] struct {
	// Topic is the default topic to produce to.
	Topic string
	// Headers is the default headers to produce with it.
	Headers []Header
	// Encode is use to marshal data to bytes. Default is json.Marshal.
	//  - If data is []byte, Encode will be ignored.
	//  - This works after Hook and record.Value is nil.
	Encode func(T) ([]byte, error)
}

func NewProducer[T any](client *Client, cfg ProducerConfig[T]) (Producer[T], error) {
	var encode func(data T) ([]byte, error)

	var value T
	switch any(value).(type) {
	case []byte:
		encode = nil
	default:
		encode = codecJSON[T]{}.Encode
	}

	setCfg := ProducerConfig[T]{
		Headers: []Header{
			{
				Key:   "server",
				Value: client.clientID,
			},
		},
		Encode: encode,
	}

	if cfg.Topic != "" {
		setCfg.Topic = cfg.Topic
	}

	if cfg.Headers != nil {
		setCfg.Headers = append(setCfg.Headers, cfg.Headers...)
	}

	if cfg.Encode != nil {
		setCfg.Encode = cfg.Encode
	}

	return &produce[T]{
		ProducerConfig: setCfg,
		produceRaw:     client.ProduceRaw,
	}, nil
}

type produce[T any] struct {
	ProducerConfig[T]
	produceRaw func(ctx context.Context, records []*Record) error
}

func (p *produce[T]) Produce(ctx context.Context, data ...T) error {
	records := make([]*Record, len(data))

	for i, d := range data {
		record, err := p.prepare(d)
		if err != nil {
			return fmt.Errorf("prepare record: %w", err)
		}

		records[i] = record
	}

	return p.produceRaw(ctx, records)
}

func (p *produce[T]) prepare(data T) (*Record, error) {
	record := &Record{
		Headers: p.Headers,
		Topic:   p.Topic,
	}

	// check data has Hook interface
	if data, ok := any(data).(ProducerHook); ok {
		data.ProduceHook(record)
	}

	if record.Value != nil {
		return record, nil
	}

	if p.Encode != nil {
		var err error
		record.Value, err = p.Encode(data)
		if err != nil {
			return nil, fmt.Errorf("encode data: %w", err)
		}
	}

	return record, nil
}
