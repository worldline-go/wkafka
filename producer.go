package wkafka

import (
	"context"
	"errors"
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

type producerConfig[T any] struct {
	// Topic is the default topic to produce to.
	Topic string
	// Headers is the default headers to produce with it.
	Headers []Header
	// Encode is use to marshal data to bytes. Default is json.Marshal.
	//  - If data is []byte, Encode will be ignored.
	//  - This works after Hook and record.Value is nil.
	Encode func(T) ([]byte, error)
	// Hook is use to modify record before produce.
	Hook func(T, *Record) error
}

type OptionProducer[T any] func(*producerConfig[T]) error

func (c *producerConfig[T]) apply(opts ...OptionProducer[T]) error {
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return err
		}
	}

	return nil
}

// WithEncoder to set encoder function.
func WithEncoder[T any](fn func(T) ([]byte, error)) OptionProducer[T] {
	return func(o *producerConfig[T]) error {
		o.Encode = fn

		return nil
	}
}

// WithHeaders to append headers.
func WithHeaders[T any](headers ...Header) OptionProducer[T] {
	return func(o *producerConfig[T]) error {
		o.Headers = append(o.Headers, headers...)

		return nil
	}
}

// WithHook to set hook function.
//   - Hook will be called before Encoder.
//   - If Hook return ErrSkip, record will be skip.
//   - If Hook not set any value to record, Encoder will be called.
func WithHook[T any](fn func(T, *Record) error) OptionProducer[T] {
	return func(o *producerConfig[T]) error {
		o.Hook = fn

		return nil
	}
}

// NewProducer to create a new procuder with type.
//   - If data is []byte, Encoder will be ignored.
func NewProducer[T any](client *Client, topic string, opts ...OptionProducer[T]) (Producer[T], error) {
	var encode func(data T) ([]byte, error)

	var value T
	switch any(value).(type) {
	case []byte:
		encode = codecByte[T]{}.Encode
	default:
		encode = codecJSON[T]{}.Encode
	}

	setCfg := &producerConfig[T]{
		Topic: topic,
		Headers: []Header{
			{
				Key:   "server",
				Value: client.clientID,
			},
		},
		Encode: encode,
	}

	if err := setCfg.apply(opts...); err != nil {
		return nil, fmt.Errorf("apply options: %w", err)
	}

	return &produce[T]{
		producerConfig: *setCfg,
		produceRaw:     client.ProduceRaw,
	}, nil
}

type produce[T any] struct {
	producerConfig[T]
	produceRaw func(ctx context.Context, records []*Record) error
}

func (p *produce[T]) Produce(ctx context.Context, data ...T) error {
	records := make([]*Record, 0, len(data))

	for _, d := range data {
		record, err := p.prepare(d)
		if err != nil {
			if errors.Is(err, ErrSkip) {
				continue
			}

			return fmt.Errorf("prepare record: %w", err)
		}

		records = append(records, record)
	}

	return p.produceRaw(ctx, records)
}

func (p *produce[T]) prepare(data T) (*Record, error) {
	record := &Record{
		Headers: p.Headers,
		Topic:   p.Topic,
	}

	if p.Hook != nil {
		if err := p.Hook(data, record); err != nil {
			return nil, err
		}
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
