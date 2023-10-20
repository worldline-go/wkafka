package wkafka

import (
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func Test_compressionOpts(t *testing.T) {
	tests := []struct {
		name    string
		c       []string
		want    []kgo.CompressionCodec
		wantErr bool
	}{
		{
			name: "valid compression options",
			c:    []string{"gzip", "snappy", "lz4", "zstd"},
			want: []kgo.CompressionCodec{
				kgo.GzipCompression(),
				kgo.SnappyCompression(),
				kgo.Lz4Compression(),
				kgo.ZstdCompression(),
				kgo.NoCompression(),
			},
			wantErr: false,
		},
		{
			name:    "invalid compression option",
			c:       []string{"gzip", "invalid", "lz4"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "empty compression option",
			c:       []string{},
			want:    []kgo.CompressionCodec{kgo.NoCompression()},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := compressionOpts(tt.c)
			if (err != nil) != tt.wantErr {
				t.Errorf("compressionOpts() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("compressionOpts() = %v, want %v", got, tt.want)
			}
		})
	}
}
