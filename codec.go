package wkafka

import (
	"encoding/json"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Compression
func compressionOpts(c []string) ([]kgo.CompressionCodec, error) {
	if err := compressionVerify(c); err != nil {
		return nil, err
	}

	opts := make([]kgo.CompressionCodec, 0, len(c)+1)
	for _, v := range c {
		switch v {
		case "gzip":
			opts = append(opts, kgo.GzipCompression())
		case "snappy":
			opts = append(opts, kgo.SnappyCompression())
		case "lz4":
			opts = append(opts, kgo.Lz4Compression())
		case "zstd":
			opts = append(opts, kgo.ZstdCompression())
		}
	}

	opts = append(opts, kgo.NoCompression())

	return opts, nil
}

func compressionVerify(c []string) error {
	for _, v := range c {
		switch v {
		case "gzip", "snappy", "lz4", "zstd":
		default:
			return fmt.Errorf("%w: %q", ErrInvalidCompression, v)
		}
	}

	return nil
}

// Codec is use to marshal/unmarshal data to bytes.

type codecJSON[T any] struct{}

func (codecJSON[T]) Encode(data T) ([]byte, error) {
	return json.Marshal(data)
}

func (codecJSON[T]) Decode(raw []byte, _ *kgo.Record) (T, error) {
	var data T
	err := json.Unmarshal(raw, &data)
	if err != nil {
		return data, err
	}

	return data, nil
}
