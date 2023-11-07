package wkafka

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

type testData struct {
	Name    string
	Topic   string
	Details map[string]interface{}
}

func (d *testData) ProduceHook(r *Record) {
	r.Value = []byte(d.Name)
	r.Headers = append(r.Headers, Header{
		Key:   "name",
		Value: []byte(d.Name),
	})
	r.Key = []byte(d.Name)
	r.Topic = d.Topic
}

func Test_produce_Produce(t *testing.T) {
	type fields[T any] struct {
		Config     ProducerConfig[T]
		ProduceRaw func(t *testing.T) func(ctx context.Context, records []*kgo.Record) error
	}
	type args struct {
		ctx  context.Context
		data []*testData
	}

	type testCase[T any] struct {
		name    string
		fields  fields[T]
		args    args
		wantErr bool
	}
	tests := []testCase[*testData]{
		{
			name: "test",
			fields: fields[*testData]{
				Config: ProducerConfig[*testData]{
					Topic: "test",
					Headers: []Header{
						{
							Key:   "server",
							Value: []byte("test"),
						},
					},
					Encode: codecJSON[*testData]{}.Encode,
				},
				ProduceRaw: func(t *testing.T) func(ctx context.Context, records []*kgo.Record) error {
					return func(ctx context.Context, records []*kgo.Record) error {
						t.Helper()

						if len(records) != 1 {
							t.Errorf("produce.Produce() len(records) = %v, want %v", len(records), 3)
						}

						for i := range records {
							switch i {
							case 0:
								assert.Equal(t, "test", records[i].Topic)
								assert.Equal(t, "test", string(records[i].Key))
								// assert.Equal(t, `{"Name":"test","Topic":"test","Details":{"key":1234}}`, string(records[i].Value))
								assert.Equal(t, `test`, string(records[i].Value))
								assert.Equal(t, 2, len(records[i].Headers))
								assert.Equal(t, "server", records[i].Headers[0].Key)
								assert.Equal(t, "test", string(records[i].Headers[0].Value))
								assert.Equal(t, "name", records[i].Headers[1].Key)
								assert.Equal(t, "test", string(records[i].Headers[1].Value))
							}
						}
						return nil
					}
				},
			},
			args: args{
				ctx: context.Background(),
				data: []*testData{
					{
						Name:  "test",
						Topic: "test",
						Details: map[string]interface{}{
							"key": 1234,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &produce[*testData]{
				ProducerConfig: tt.fields.Config,
				produceRaw:     tt.fields.ProduceRaw(t),
			}

			if err := p.Produce(tt.args.ctx, tt.args.data...); (err != nil) != tt.wantErr {
				t.Errorf("produce.Produce() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func BenchmarkProduce(b *testing.B) {
	p := &produce[*testData]{
		ProducerConfig: ProducerConfig[*testData]{
			Topic: "test",
			Headers: []Header{
				{
					Key:   "server",
					Value: []byte("test"),
				},
			},
		},
		produceRaw: func(ctx context.Context, records []*kgo.Record) error {
			return nil
		},
	}

	datas := []*testData{
		nil,
		// []byte("test"),
		{
			Name:  "test",
			Topic: "test",
			Details: map[string]interface{}{
				"key": 1234,
			},
		},
		{
			Name:  "test",
			Topic: "test",
			Details: map[string]interface{}{
				"key": 1234,
			},
		},
	}

	for i := 0; i < b.N; i++ {
		if err := p.Produce(context.Background(), datas...); err != nil {
			b.Error(err)
		}
	}
}
