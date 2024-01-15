package wkafka

import (
	"context"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/worldline-go/logz"
)

type partitionHandler struct {
	logger logz.Adapter

	mapPartitionsRevoked map[string][]int32
	mapPartitionsLost    map[string][]int32

	mutex sync.RWMutex
}

// Flush is used to flush the partition handler and it will be ready next poll.
func (h *partitionHandler) Flush() {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	h.mapPartitionsRevoked = nil
	h.mapPartitionsLost = nil
}

func (h *partitionHandler) AddPartitionsRevoked(mapPartitions map[string][]int32) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.mapPartitionsRevoked == nil {
		h.mapPartitionsRevoked = make(map[string][]int32, len(mapPartitions))
	}

	for topic, partition := range mapPartitions {
		h.mapPartitionsRevoked[topic] = partition
	}
}

func (h *partitionHandler) AddPartitionsLost(mapPartitions map[string][]int32) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.mapPartitionsLost == nil {
		h.mapPartitionsLost = make(map[string][]int32, len(mapPartitions))
	}

	for topic, partition := range mapPartitions {
		h.mapPartitionsLost[topic] = partition
	}
}

func (h *partitionHandler) IsRevokedRecord(r *Record) bool {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

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

// IsRevokedRecordBatch is used to check if the record is revoked.
//   - If the record is revoked, it will be skipped and returns just valid records.
func (h *partitionHandler) IsRevokedRecordBatch(records []*Record) ([]*Record, bool) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()

	if len(h.mapPartitionsRevoked) == 0 {
		return records, false
	}

	validRecords := make([]*Record, 0, len(records))
	for _, r := range records {
		if _, ok := h.mapPartitionsRevoked[r.Topic]; !ok {
			validRecords = append(validRecords, r)
			continue
		}

		for _, partition := range h.mapPartitionsRevoked[r.Topic] {
			if partition == r.Partition {
				continue
			}

			validRecords = append(validRecords, r)
		}
	}

	return validRecords, len(validRecords) != len(records)
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
