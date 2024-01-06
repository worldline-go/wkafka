package wkafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func Test_errorOffsetList(t *testing.T) {
	type args struct {
		r []*kgo.Record
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "empty",
			args: args{
				r: []*kgo.Record{},
			},
			want: `{}`,
		},
		{
			name: "one",
			args: args{
				r: []*kgo.Record{
					{
						Topic:     "topic",
						Partition: 1,
						Offset:    2,
					},
				},
			},
			want: `{"topic":{"1":"2"}}`,
		},
		{
			name: "multiple",
			args: args{
				r: []*kgo.Record{
					{
						Topic:     "topic",
						Partition: 1,
						Offset:    2,
					},
					{
						Topic:     "topic",
						Partition: 1,
						Offset:    3,
					},
					{
						Topic:     "topic",
						Partition: 1,
						Offset:    4,
					},
					{
						Topic:     "topic",
						Partition: 1,
						Offset:    8,
					},
					{
						Topic:     "topic",
						Partition: 2,
						Offset:    4,
					},
					{
						Topic:     "topic2",
						Partition: 1,
						Offset:    2,
					},
				},
			},
			want: `{"topic":{"1":"2-4,8","2":"4"},"topic2":{"1":"2"}}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := errorOffsetList(tt.args.r); got != tt.want {
				t.Errorf("errorOffsetList() = %v, want %v", got, tt.want)
			}
		})
	}
}
