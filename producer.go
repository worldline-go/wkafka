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

type producerConfigInf interface{}

type OptionProducer func(producerConfigInf) error

func (c *producerConfig[T]) apply(opts ...OptionProducer) error {
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return err
		}
	}

	return nil
}

// WithEncoder to set encoder function.
func WithEncoder[T any](fn func(T) ([]byte, error)) OptionProducer {
	return func(o producerConfigInf) error {
		v := o.(*producerConfig[T])
		v.Encode = fn

		return nil
	}
}

// WithHeaders to append headers.
func WithHeaders[T any](headers ...Header) OptionProducer {
	return func(o producerConfigInf) error {
		v := o.(*producerConfig[T])
		v.Headers = append(v.Headers, headers...)

		return nil
	}
}

// WithHook to set hook function.
//   - Hook will be called before Encoder.
//   - If Hook return ErrSkip, record will be skip.
//   - If Hook not set any value to record, Encoder will be called.
func WithHook[T any](fn func(T, *Record) error) OptionProducer {
	return func(o producerConfigInf) error {
		v := o.(*producerConfig[T])
		v.Hook = fn

		return nil
	}
}

// NewProducer to create a new procuder with type.
//
//   - If data is []byte, Encoder will be ignored.
//   - WithHook, WithEncoder, WithHeaders options can be used.
func NewProducer[T any](client *Client, topic string, opts ...OptionProducer) (*Producer[T], error) {
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

	return &Producer[T]{
		config:     *setCfg,
		produceRaw: client.ProduceRaw,
	}, nil
}

type Producer[T any] struct {
	config     producerConfig[T]
	produceRaw func(ctx context.Context, records []*Record) error
}

func (p *Producer[T]) Produce(ctx context.Context, data ...T) error {
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

func (p *Producer[T]) prepare(data T) (*Record, error) {
	record := &Record{
		Headers: p.config.Headers,
		Topic:   p.config.Topic,
	}

	if p.config.Hook != nil {
		if err := p.config.Hook(data, record); err != nil {
			return nil, err
		}
	}

	if record.Value != nil {
		return record, nil
	}

	if p.config.Encode != nil {
		var err error
		record.Value, err = p.config.Encode(data)
		if err != nil {
			return nil, fmt.Errorf("encode data: %w", err)
		}
	}

	return record, nil
}
