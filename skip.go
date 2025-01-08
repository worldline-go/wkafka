package wkafka

import (
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

func ToPtr[T any](v T) *T {
	return &v
}

func newSkipper(c *sync.RWMutex) func(cfg *ConsumerConfig, r *kgo.Record) bool {
	return func(cfg *ConsumerConfig, r *kgo.Record) bool {
		c.RLock()
		defer c.RUnlock()

		return skipCheck(cfg.Skip, r)
	}
}

func skipCheck(skip map[string]map[int32]OffsetConfig, r *kgo.Record) bool {
	if len(skip) == 0 {
		return false
	}

	topic, ok := skip[r.Topic]
	if !ok {
		return false
	}

	offsets, ok := topic[r.Partition]
	if !ok {
		return false
	}

	if offsets.Before != nil && r.Offset <= *offsets.Before {
		return true
	}

	for _, offset := range offsets.Offsets {
		if r.Offset == offset {
			return true
		}
	}

	return false
}

func SkipReplace(skip SkipMap) func(SkipMap) SkipMap {
	return func(m SkipMap) SkipMap {
		return skip
	}
}

func SkipAppend(skip SkipMap) func(SkipMap) SkipMap {
	return func(m SkipMap) SkipMap {
		if m == nil {
			return skip
		}

		for topic, partitions := range skip {
			if v, ok := m[topic]; !ok || v == nil {
				m[topic] = make(map[int32]OffsetConfig)
			}

			for partition, offset := range partitions {
				if _, ok := m[topic][partition]; !ok {
					m[topic][partition] = offset
				} else {
					m[topic][partition] = OffsetConfig{
						Before:  offset.Before,
						Offsets: append(m[topic][partition].Offsets, offset.Offsets...),
					}
				}
			}
		}

		return m
	}
}
