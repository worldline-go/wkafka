package wkafka

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Compression

// compressionParse converts a single compression config entry to a kafka codec.
//
// The accepted format is "<codec>" or "<codec>:<level>". Codec names are
// case insensitive and surrounding spaces are ignored.
//
//	Available codecs: none, gzip, snappy, lz4, zstd
//
// A level is only accepted for gzip, lz4 and zstd; snappy and none have no
// level. An out of range level falls back to the codec's default level.
func compressionParse(v string) (kgo.CompressionCodec, error) {
	name, level, hasLevel := strings.Cut(v, ":")
	name = strings.ToLower(strings.TrimSpace(name))

	var (
		codec       kgo.CompressionCodec
		levelUsable bool
	)

	switch name {
	case "none", "uncompressed":
		codec = kgo.NoCompression()
	case "snappy":
		codec = kgo.SnappyCompression()
	case "gzip":
		codec, levelUsable = kgo.GzipCompression(), true
	case "lz4":
		codec, levelUsable = kgo.Lz4Compression(), true
	case "zstd":
		codec, levelUsable = kgo.ZstdCompression(), true
	default:
		return codec, fmt.Errorf("invalid compression: %q", v)
	}

	if !hasLevel {
		return codec, nil
	}

	if !levelUsable {
		return codec, fmt.Errorf("invalid compression %q: %q has no level", v, name)
	}

	levelNum, err := strconv.Atoi(strings.TrimSpace(level))
	if err != nil {
		return codec, fmt.Errorf("invalid compression %q: level %q is not a number", v, level)
	}

	return codec.WithLevel(levelNum), nil
}

// compressionOpts converts the compression config to the kafka codec
// preference list. Codecs are tried in the given order based on broker
// support, so kgo.NoCompression is always appended as the last fallback.
func compressionOpts(c []string) ([]kgo.CompressionCodec, error) {
	opts := make([]kgo.CompressionCodec, 0, len(c)+1)

	for _, v := range c {
		codec, err := compressionParse(v)
		if err != nil {
			return nil, err
		}

		opts = append(opts, codec)
	}

	return append(opts, kgo.NoCompression()), nil
}

// Codec is use to marshal/unmarshal data to bytes.

type codecJSON[T any] struct {
	SkipInvalid bool
}

func (codecJSON[T]) Encode(data T) ([]byte, error) {
	return json.Marshal(data)
}

func (c codecJSON[T]) Decode(raw []byte, _ *kgo.Record) (T, error) {
	var data T
	if err := json.Unmarshal(raw, &data); err != nil {
		if c.SkipInvalid {
			return data, fmt.Errorf("json decoder invalid data: %w; %w", err, ErrSkip)
		}

		return data, err
	}

	return data, nil
}

type codecByte[T any] struct{}

func (codecByte[T]) Encode(data T) ([]byte, error) {
	v, ok := any(data).([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid data type: %T", data)
	}

	return v, nil
}

func (codecByte[T]) Decode(raw []byte, _ *kgo.Record) (T, error) {
	return any(raw).(T), nil
}
