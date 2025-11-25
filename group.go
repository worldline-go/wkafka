package wkafka

import (
	"fmt"
	"iter"
	"strconv"
)

type group struct {
	Type      groupType
	BatchSize int
	MinSize   int // Minimum size of the group to process
	RunSize   int // Number of messages to process in a single run
}

type groupType int

const (
	groupTypeKey       groupType = iota // Process by key
	groupTypePartition                  // Process by partition
	groupTypeMix                        // Process by key and partition
)

const (
	GroupTypeKeyStr       string = "key"
	GroupTypePartitionStr string = "partition"
	GroupTypeMixStr       string = "mix"
)

func groupTypeFromString(s string) (groupType, error) {
	switch s {
	case GroupTypeKeyStr:
		return groupTypeKey, nil
	case GroupTypePartitionStr:
		return groupTypePartition, nil
	case GroupTypeMixStr:
		return groupTypeMix, nil
	default:
		return 0, fmt.Errorf("unknown group type: %s", s)
	}
}

func (c groupType) String() string {
	switch c {
	case groupTypeKey:
		return GroupTypeKeyStr
	case groupTypePartition:
		return GroupTypePartitionStr
	case groupTypeMix:
		return GroupTypeMixStr
	}

	return ""
}

// ///////////////

type groupRecords interface {
	// Add adds a record to the group
	Add(r *Record)
	// IsEnough returns true if group has enough records to process
	IsEnough() bool
	// Reset clears the group records
	Reset()
	// AllRecords returns all records in the group
	AllRecords() []*Record
	// Iter returns an iterator over the records in the group
	Iter() iter.Seq[[]*Record]
	// IsSingle returns true if group can be processed as single record in goroutine
	IsSingle() bool
	// Merge for combining groups if size is less
	Merge()
}

type groupKey struct {
	size       int
	runSize    int
	min        int
	count      int
	records    map[string][]*Record
	allRecords []*Record
}

func newGroupKey(size, runSize, min int) *groupKey {
	if min <= 0 {
		min = 1
	}

	if runSize <= 0 {
		runSize = size
	}

	return &groupKey{
		size:       size,
		runSize:    runSize,
		min:        min,
		records:    make(map[string][]*Record),
		allRecords: make([]*Record, 0, runSize),
	}
}

func (c *groupKey) Add(r *Record) {
	key := string(r.Key)
	if _, ok := c.records[key]; !ok {
		c.records[key] = make([]*Record, 0, c.size)
	}

	c.count++
	c.records[key] = append(c.records[key], r)
	c.allRecords = append(c.allRecords, r)
}

func (c *groupKey) IsEnough() bool {
	return c.count >= c.runSize
}

func (c *groupKey) Reset() {
	c.allRecords = c.allRecords[:0]
	c.records = make(map[string][]*Record)
	c.count = 0
}

func (c *groupKey) AllRecords() []*Record {
	return c.allRecords
}

func (c *groupKey) Iter() iter.Seq[[]*Record] {
	return func(yield func([]*Record) bool) {
		for _, records := range c.records {
			if len(records) == 0 {
				continue
			}

			if !yield(records) {
				return
			}
		}
	}
}

func (c *groupKey) IsSingle() bool {
	return false
}

func (c *groupKey) Merge() {
	mergeGroup(c.min, c.records)
}

type groupPartition struct {
	size       int
	runSize    int
	min        int
	count      int
	records    map[int32][]*Record
	allRecords []*Record
}

func newGroupPartition(size, runSize, min int) *groupPartition {
	if min <= 0 {
		min = 1
	}

	if runSize <= 0 {
		runSize = size
	}

	return &groupPartition{
		size:       size,
		runSize:    runSize,
		min:        min,
		records:    make(map[int32][]*Record),
		allRecords: make([]*Record, 0, runSize),
	}
}

func (c *groupPartition) Add(r *Record) {
	c.count++

	c.allRecords = append(c.allRecords, r)

	if _, ok := c.records[r.Partition]; !ok {
		c.records[r.Partition] = make([]*Record, 0, c.size)
	}

	c.records[r.Partition] = append(c.records[r.Partition], r)
}

func (c *groupPartition) IsEnough() bool {
	return c.count >= c.runSize
}

func (c *groupPartition) Reset() {
	c.allRecords = c.allRecords[:0]
	c.records = make(map[int32][]*Record)
	c.count = 0
}

func (c *groupPartition) AllRecords() []*Record {
	return c.allRecords
}

func (c *groupPartition) Iter() iter.Seq[[]*Record] {
	return func(yield func([]*Record) bool) {
		for _, records := range c.records {
			if len(records) == 0 {
				continue
			}

			if !yield(records) {
				return
			}
		}
	}
}

func (c *groupPartition) IsSingle() bool {
	return false
}

// Merge combines groups if size is less than min
func (c *groupPartition) Merge() {
	mergeGroup(c.min, c.records)
}

type groupMix struct {
	size    int
	batch   int
	records []*Record
}

func newGroupMix(batch, size int) *groupMix {
	return &groupMix{
		size:    size,
		batch:   batch,
		records: make([]*Record, 0, size),
	}
}

func (c *groupMix) Add(r *Record) {
	c.records = append(c.records, r)
}

func (c *groupMix) IsEnough() bool {
	return len(c.records) >= c.size
}

func (c *groupMix) Reset() {
	c.records = c.records[:0]
}

func (c *groupMix) AllRecords() []*Record {
	return c.records
}

func (c *groupMix) Iter() iter.Seq[[]*Record] {
	if c.size != c.batch {
		// Return batches of records
		return func(yield func([]*Record) bool) {
			total := len(c.records)
			for i := 0; i < total; i += c.batch {
				end := min(i+c.batch, total)

				if !yield(c.records[i:end]) {
					return
				}
			}
		}
	}

	return func(yield func([]*Record) bool) {
		if !yield(c.records) {
			return
		}
	}
}

func (c *groupMix) IsSingle() bool {
	return true
}

func (c *groupMix) Merge() {}

// ///////////////

func (c *group) NewGroup() groupRecords {
	switch c.Type {
	case groupTypeKey:
		return newGroupKey(c.BatchSize, c.RunSize, c.MinSize)
	case groupTypePartition:
		return newGroupPartition(c.BatchSize, c.RunSize, c.MinSize)
	case groupTypeMix:
		return newGroupMix(c.BatchSize, c.RunSize)
	default:
		panic("unknown concurrent type: " + strconv.Itoa(int(c.Type)))
	}
}

// ////////////////

func mergeGroup[T comparable](min int, m map[T][]*Record) {
	var shortLists []T
	var validTargets []T

	for key, list := range m {
		if len(list) < min {
			shortLists = append(shortLists, key)
		} else {
			validTargets = append(validTargets, key)
		}
	}

	// If no valid targets exist, find the longest list as fallback
	if len(validTargets) == 0 && len(shortLists) > 1 {
		// Find the longest list among short lists to use as target
		longestKey := shortLists[0]
		maxLen := len(m[longestKey])

		for _, key := range shortLists[1:] {
			if len(m[key]) > maxLen {
				maxLen = len(m[key])
				longestKey = key
			}
		}

		validTargets = append(validTargets, longestKey)
		// Remove the target from shortLists
		for i, key := range shortLists {
			if key == longestKey {
				shortLists = append(shortLists[:i], shortLists[i+1:]...)

				break
			}
		}
	}

	// Move elements from short lists to target lists
	for _, shortKey := range shortLists {
		if len(validTargets) == 0 {
			continue // No valid targets available
		}

		// Find target with minimum length
		targetKey := findMinLengthList(m, validTargets)

		// Move all elements from short list to target list
		m[targetKey] = append(m[targetKey], m[shortKey]...)

		// Remove the short list
		delete(m, shortKey)
	}
}

func findMinLengthList[T comparable](m map[T][]*Record, candidates []T) T {
	sortest := candidates[0]
	for _, key := range candidates {
		if len(m[key]) < len(m[sortest]) {
			sortest = key
		}
	}

	return sortest
}
