package wkafka

import (
	"context"
	"reflect"
	"testing"
)

func TestCtxRecordWithIndex(t *testing.T) {
	type args struct {
		ctx   context.Context
		index int
	}
	tests := []struct {
		name string
		args args
		want *Record
	}{
		{
			name: "dlq",
			args: args{
				ctx:   context.WithValue(context.WithValue(context.Background(), KeyIsDLQProcess, true), KeyRecord, &Record{}),
				index: 0,
			},
			want: &Record{},
		},
		{
			name: "with batch",
			args: args{
				ctx: context.WithValue(context.Background(), KeyRecord, []*Record{{}, {}, {
					Key: []byte("test"),
				}}),
				index: 2,
			},
			want: &Record{
				Key: []byte("test"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CtxRecordWithIndex(tt.args.ctx, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CtxRecordWithIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}
