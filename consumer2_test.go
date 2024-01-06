package wkafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func Test_skip(t *testing.T) {
	type args struct {
		cfg *ConsumerConfig
		r   *kgo.Record
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "empty",
			args: args{
				cfg: &ConsumerConfig{},
				r:   &kgo.Record{},
			},
			want: false,
		},
		{
			name: "empty config",
			args: args{
				cfg: &ConsumerConfig{},
				r: &kgo.Record{
					Topic:     "topic",
					Partition: 0,
					Offset:    5,
				},
			},
			want: false,
		},
		{
			name: "skip topic",
			args: args{
				cfg: &ConsumerConfig{
					Skip: map[string]map[int32]Offsets{
						"topic": {
							0: {
								Offsets: []int64{
									5,
								},
							},
						},
					},
				},
				r: &kgo.Record{
					Topic:     "topic",
					Partition: 0,
					Offset:    5,
				},
			},
			want: true,
		},
		{
			name: "skip topic before",
			args: args{
				cfg: &ConsumerConfig{
					Skip: map[string]map[int32]Offsets{
						"topic": {
							0: {
								Before: 5,
							},
						},
					},
				},
				r: &kgo.Record{
					Topic:     "topic",
					Partition: 0,
					Offset:    5,
				},
			},
			want: true,
		},
		{
			name: "topic before",
			args: args{
				cfg: &ConsumerConfig{
					Skip: map[string]map[int32]Offsets{
						"topic": {
							0: {
								Before: 5,
								Offsets: []int64{
									9,
									10,
								},
							},
						},
					},
				},
				r: &kgo.Record{
					Topic:     "topic",
					Partition: 0,
					Offset:    6,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := skip(tt.args.cfg, tt.args.r); got != tt.want {
				t.Errorf("skip() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_skipDLQ(t *testing.T) {
	type args struct {
		cfg *ConsumerConfig
		r   *kgo.Record
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "empty",
			args: args{
				cfg: &ConsumerConfig{},
				r:   &kgo.Record{},
			},
			want: false,
		},
		{
			name: "empty config",
			args: args{
				cfg: &ConsumerConfig{},
				r: &kgo.Record{
					Topic:     "topic",
					Partition: 0,
					Offset:    5,
				},
			},
			want: false,
		},
		{
			name: "skip topic",
			args: args{
				cfg: &ConsumerConfig{
					DLQ: DLQ{
						SkipExtra: map[string]map[int32]Offsets{
							"topic": {
								0: {
									Offsets: []int64{
										5,
									},
								},
							},
						},
					},
				},
				r: &kgo.Record{
					Topic:     "topic",
					Partition: 0,
					Offset:    5,
				},
			},
			want: true,
		},
		{
			name: "skip topic before",
			args: args{
				cfg: &ConsumerConfig{
					DLQ: DLQ{
						SkipExtra: map[string]map[int32]Offsets{
							"topic": {
								0: {
									Before: 5,
								},
							},
						},
					},
				},
				r: &kgo.Record{
					Topic:     "topic",
					Partition: 0,
					Offset:    5,
				},
			},
			want: true,
		},
		{
			name: "topic before",
			args: args{
				cfg: &ConsumerConfig{
					DLQ: DLQ{
						SkipExtra: map[string]map[int32]Offsets{
							"topic": {
								0: {
									Before: 5,
									Offsets: []int64{
										9,
										10,
									},
								},
							},
						},
					},
				},
				r: &kgo.Record{
					Topic:     "topic",
					Partition: 0,
					Offset:    6,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := skipDLQ(tt.args.cfg, tt.args.r); got != tt.want {
				t.Errorf("skipDLQ() = %v, want %v", got, tt.want)
			}
		})
	}
}
