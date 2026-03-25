package wkafka

import (
	"context"
	"slices"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type partitionHandler struct {
	logger Logger

	mapPartitionsRevoked map[string][]int32
	mapPartitionsLost    map[string][]int32

	rewindMutex             sync.Mutex
	rewindPartitionsOffsets map[string]map[int32]int64
	resetRewindAtNextPoll   bool

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

	for k, v := range mapPartitions {
		h.mapPartitionsRevoked[k] = append(h.mapPartitionsRevoked[k], v...)
	}
}

func (h *partitionHandler) AddPartitionsLost(mapPartitions map[string][]int32) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	if h.mapPartitionsLost == nil {
		h.mapPartitionsLost = make(map[string][]int32, len(mapPartitions))
	}

	for k, v := range mapPartitions {
		h.mapPartitionsLost[k] = append(h.mapPartitionsLost[k], v...)
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

	return slices.Contains(h.mapPartitionsRevoked[r.Topic], r.Partition)
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

		if !slices.Contains(h.mapPartitionsRevoked[r.Topic], r.Partition) {
			validRecords = append(validRecords, r)
		}
	}

	return validRecords, len(validRecords) != len(records)
}

func partitionLost(h *partitionHandler, fn func(...OptionDLQTriggerFn)) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
		if len(partitions) == 0 {
			return
		}

		h.logger.Info("partition lost", "partitions", partitions)

		h.AddPartitionsLost(partitions)

		fn(WithDLQTriggerSpecPartitions(partitions))
	}
}

func partitionRevoked(h *partitionHandler, fn func(...OptionDLQTriggerFn)) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
		if len(partitions) == 0 {
			return
		}

		h.logger.Info("partition revoked", "partitions", partitions)

		// After any partition is revoked, all the buffered fetches are dropped, so we don't need to rewind anymore.
		h.rewindMutex.Lock()
		defer h.rewindMutex.Unlock()

		h.resetRewindAtNextPoll = true

		h.AddPartitionsRevoked(partitions)

		fn(WithDLQTriggerSpecPartitions(partitions))
	}
}

func partitionsAssigned(h *partitionHandler) func(context.Context, *kgo.Client, map[string][]int32) {
	return func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
		if len(partitions) == 0 {
			return
		}

		h.logger.Info("partitions assigned", "partitions", partitions)
	}
}

// rewindPartitionToUncommittedOffset sets an offset that we want to rewind to on specific partition of the topic.
// It should not be used to rewind to already committed offset.
func (h *partitionHandler) rewindPartitionToUncommittedOffset(topic string, partition int32, offset int64) {
	h.logger.Info("rewind partition to old offset", "topic", topic, "partition", partition, "offset", offset)

	h.rewindMutex.Lock()
	defer h.rewindMutex.Unlock()

	if h.rewindPartitionsOffsets == nil {
		h.rewindPartitionsOffsets = make(map[string]map[int32]int64)
	}

	_, ok := h.rewindPartitionsOffsets[topic]
	if !ok {
		h.rewindPartitionsOffsets[topic] = make(map[int32]int64)
	}

	h.rewindPartitionsOffsets[topic][partition] = offset
}

// isPartitionRewinding checks if that specific partition of the topic is set to be rewound.
func (h *partitionHandler) isPartitionRewinding(topic string, partition int32) bool {
	h.rewindMutex.Lock()
	defer h.rewindMutex.Unlock()

	if h.rewindPartitionsOffsets == nil {
		return false
	}

	partitions, ok := h.rewindPartitionsOffsets[topic]
	if !ok {
		return false
	}

	_, ok = partitions[partition]

	return ok
}

// shouldSkipRecord checks if the record should be skipped because of rewinding process.
func (h *partitionHandler) shouldSkipRecord(r *kgo.Record) bool {
	h.rewindMutex.Lock()
	defer h.rewindMutex.Unlock()

	if h.rewindPartitionsOffsets == nil {
		return false
	}

	partitions, ok := h.rewindPartitionsOffsets[r.Topic]
	if !ok {
		return false
	}

	offset, ok := partitions[r.Partition]
	if !ok {
		return false
	}

	return r.Offset > offset
}

// markPartitionRewound removes partition from the map of the partitions to be rewound.
func (h *partitionHandler) markPartitionRewound(topic string, partition int32) {
	h.rewindMutex.Lock()
	defer h.rewindMutex.Unlock()

	if h.rewindPartitionsOffsets == nil {
		return
	}

	if _, ok := h.rewindPartitionsOffsets[topic]; !ok {
		return
	}

	delete(h.rewindPartitionsOffsets[topic], partition)
}

// shouldResetRewind returns true if the buffered fetches has been discarded and the rewind is not necessary anymore.
func (h *partitionHandler) shouldResetRewind() bool {
	h.rewindMutex.Lock()
	defer h.rewindMutex.Unlock()

	return h.resetRewindAtNextPoll
}

// resetRewind resets the rewind process for all the topics and partitions.
func (h *partitionHandler) resetRewind() {
	h.rewindMutex.Lock()
	defer h.rewindMutex.Unlock()

	h.rewindPartitionsOffsets = make(map[string]map[int32]int64)

	h.resetRewindAtNextPoll = false
}
