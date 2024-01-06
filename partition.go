package wkafka

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/worldline-go/logz"
)

type partitionHandler struct {
	logger logz.Adapter

	mapPartitionsRevoked map[string][]int32
	mapPartitionsLost    map[string][]int32
}

// Flush is used to flush the partition handler and it will be ready next poll.
func (h *partitionHandler) Flush() {
	h.mapPartitionsRevoked = nil
	h.mapPartitionsLost = nil
}

func (h *partitionHandler) AddPartitionsRevoked(mapPartitions map[string][]int32) {
	if h.mapPartitionsRevoked == nil {
		h.mapPartitionsRevoked = make(map[string][]int32, len(mapPartitions))
	}

	for topic, partition := range mapPartitions {
		h.mapPartitionsRevoked[topic] = partition
	}
}

func (h *partitionHandler) AddPartitionsLost(mapPartitions map[string][]int32) {
	if h.mapPartitionsLost == nil {
		h.mapPartitionsLost = make(map[string][]int32, len(mapPartitions))
	}

	for topic, partition := range mapPartitions {
		h.mapPartitionsLost[topic] = partition
	}
}

func (h *partitionHandler) IsRevokedRecord(r *Record) bool {
	if len(h.mapPartitionsRevoked) == 0 {
		return false
	}

	if _, ok := h.mapPartitionsRevoked[r.Topic]; !ok {
		return false
	}

	for _, partition := range h.mapPartitionsRevoked[r.Topic] {
		if partition == r.Partition {
			return true
		}
	}

	return false
}

func partitionLost(h *partitionHandler) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(ctx context.Context, cl *kgo.Client, partitions map[string][]int32) {
		if len(partitions) == 0 {
			return
		}

		h.logger.Debug("partition lost", "partitions", partitions)

		h.AddPartitionsLost(partitions)
	}
}

func partitionRevoked(h *partitionHandler) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(ctx context.Context, cl *kgo.Client, partitions map[string][]int32) {
		if len(partitions) == 0 {
			return
		}

		h.logger.Debug("partition revoked", "partitions", partitions)

		h.AddPartitionsRevoked(partitions)
	}
}
