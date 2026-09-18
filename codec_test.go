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
		{
			name: "case insensitive and spaced",
			c:    []string{" GZIP ", "Snappy"},
			want: []kgo.CompressionCodec{
				kgo.GzipCompression(),
				kgo.SnappyCompression(),
				kgo.NoCompression(),
			},
			wantErr: false,
		},
		{
			name: "none is a valid codec",
			c:    []string{"zstd", "none"},
			want: []kgo.CompressionCodec{
				kgo.ZstdCompression(),
				kgo.NoCompression(),
				kgo.NoCompression(),
			},
			wantErr: false,
		},
		{
			name: "with levels",
			c:    []string{"zstd:3", "gzip: 9", "lz4:2"},
			want: []kgo.CompressionCodec{
				kgo.ZstdCompression().WithLevel(3),
				kgo.GzipCompression().WithLevel(9),
				kgo.Lz4Compression().WithLevel(2),
				kgo.NoCompression(),
			},
			wantErr: false,
		},
		{
			name:    "level on snappy",
			c:       []string{"snappy:3"},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "level not a number",
			c:       []string{"zstd:high"},
			want:    nil,
			wantErr: true,
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
