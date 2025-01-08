package wkafka

import (
	"reflect"
	"sync"
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
					Skip: map[string]map[int32]OffsetConfig{
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
					Skip: map[string]map[int32]OffsetConfig{
						"topic": {
							0: {
								Before: ToPtr(int64(5)),
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
			name: "skip topic before zero",
			args: args{
				cfg: &ConsumerConfig{
					Skip: map[string]map[int32]OffsetConfig{
						"topic": {
							0: {
								Before: ToPtr(int64(0)),
							},
						},
					},
				},
				r: &kgo.Record{
					Topic:     "topic",
					Partition: 0,
					Offset:    0,
				},
			},
			want: true,
		},
		{
			name: "topic before",
			args: args{
				cfg: &ConsumerConfig{
					Skip: map[string]map[int32]OffsetConfig{
						"topic": {
							0: {
								Before: ToPtr(int64(5)),
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

	var cfgMutex sync.RWMutex
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newSkipper(&cfgMutex)(tt.args.cfg, tt.args.r); got != tt.want {
				t.Errorf("skip() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSkipAppend(t *testing.T) {
	type args struct {
		skip SkipMap
		base SkipMap
	}
	tests := []struct {
		name string
		args args
		want SkipMap
	}{
		{
			name: "empty",
			args: args{
				skip: nil,
			},
			want: nil,
		},
		{
			name: "mixed",
			args: args{
				base: map[string]map[int32]OffsetConfig{
					"topic2": {
						0: {
							Before:  ToPtr(int64(10)),
							Offsets: []int64{1, 2, 3},
						},
					},
					"topic": {
						0: {
							Offsets: []int64{11},
						},
					},
				},
				skip: map[string]map[int32]OffsetConfig{
					"topic2": {
						1: {
							Before:  ToPtr(int64(5)),
							Offsets: []int64{9, 10},
						},
					},
					"topic": {
						0: {
							Before: ToPtr(int64(5)),
							Offsets: []int64{
								9,
								10,
							},
						},
					},
				},
			},
			want: map[string]map[int32]OffsetConfig{
				"topic2": {
					0: {
						Before:  ToPtr(int64(10)),
						Offsets: []int64{1, 2, 3},
					},
					1: {
						Before:  ToPtr(int64(5)),
						Offsets: []int64{9, 10},
					},
				},
				"topic": {
					0: {
						Before: ToPtr(int64(5)),
						Offsets: []int64{
							11,
							9,
							10,
						},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SkipAppend(tt.args.skip)(tt.args.base); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SkipAppend() = %v, want %v", got, tt.want)
			}
		})
	}
}
