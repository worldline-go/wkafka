package wkafka

import (
	"bytes"
	"fmt"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"

	"golang.org/x/sync/errgroup"
)

func TestGroup(t *testing.T) {
	type args struct {
		groupType string
		batchSize int
		minSize   int
	}

	tests := []struct {
		name        string
		args        args
		records     []*Record
		wantGroup   group
		wantRecords [][]*Record
		wantErr     bool
	}{
		{
			name: "valid group type key",
			args: args{
				groupType: "key",
				batchSize: 100,
				minSize:   1,
			},
			records: []*Record{
				{Key: []byte("key1"), Value: []byte("value1")},
				{Key: []byte("key2"), Value: []byte("value2")},
				{Key: []byte("key2"), Value: []byte("value3")},
			},
			wantRecords: [][]*Record{
				{
					{Key: []byte("key1"), Value: []byte("value1")},
				},
				{
					{Key: []byte("key2"), Value: []byte("value2")},
					{Key: []byte("key2"), Value: []byte("value3")},
				},
			},
			wantGroup: group{
				Type:      groupTypeKey,
				BatchSize: 100,
				MinSize:   1,
			},
		},
		{
			name: "valid group type partition",
			args: args{
				groupType: "partition",
				batchSize: 100,
				minSize:   1,
			},
			records: []*Record{
				{Partition: 0, Key: []byte("key1"), Value: []byte("value1")},
				{Partition: 1, Key: []byte("key2"), Value: []byte("value2")},
				{Partition: 1, Key: []byte("key2"), Value: []byte("value3")},
			},
			wantRecords: [][]*Record{
				{
					{Partition: 0, Key: []byte("key1"), Value: []byte("value1")},
				},
				{
					{Partition: 1, Key: []byte("key2"), Value: []byte("value2")},
					{Partition: 1, Key: []byte("key2"), Value: []byte("value3")},
				},
			},
			wantGroup: group{
				Type:      groupTypePartition,
				BatchSize: 100,
				MinSize:   1,
			},
		},
		{
			name: "valid group type mix",
			args: args{
				groupType: "mix",
				batchSize: 100,
				minSize:   1,
			},
			records: []*Record{
				{Key: []byte("key1"), Partition: 0, Value: []byte("value1")},
				{Key: []byte("key2"), Partition: 1, Value: []byte("value2")},
				{Key: []byte("key2"), Partition: 1, Value: []byte("value3")},
			},
			wantRecords: [][]*Record{
				{
					{Key: []byte("key1"), Partition: 0, Value: []byte("value1")},
					{Key: []byte("key2"), Partition: 1, Value: []byte("value2")},
					{Key: []byte("key2"), Partition: 1, Value: []byte("value3")},
				},
			},
			wantGroup: group{
				Type:      groupTypeMix,
				BatchSize: 100,
				MinSize:   1,
			},
		},
		{
			name: "valid group type key min size 2",
			args: args{
				groupType: "key",
				batchSize: 100,
				minSize:   2,
			},
			records: []*Record{
				{Key: []byte("key1"), Value: []byte("value1")},
				{Key: []byte("key2"), Value: []byte("value2")},
				{Key: []byte("key2"), Value: []byte("value3")},
				{Key: []byte("key2"), Value: []byte("value4")},
				{Key: []byte("key3"), Value: []byte("value5")},
				{Key: []byte("key3"), Value: []byte("value6")},
			},
			wantRecords: [][]*Record{
				{
					{Key: []byte("key2"), Value: []byte("value2")},
					{Key: []byte("key2"), Value: []byte("value3")},
					{Key: []byte("key2"), Value: []byte("value4")},
				},
				{
					{Key: []byte("key3"), Value: []byte("value5")},
					{Key: []byte("key3"), Value: []byte("value6")},
					{Key: []byte("key1"), Value: []byte("value1")},
				},
			},
			wantGroup: group{
				Type:      groupTypeKey,
				BatchSize: 100,
				MinSize:   2,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groupType, err := groupTypeFromString(tt.args.groupType)
			if tt.wantErr != (err != nil) {
				t.Fatalf("groupTypeFromString() error = %v, wantErr %v", err, tt.wantErr)
			}

			group := group{
				Type:      groupType,
				BatchSize: tt.args.batchSize,
				MinSize:   tt.args.minSize,
			}

			if group.Type != tt.wantGroup.Type || group.BatchSize != tt.wantGroup.BatchSize || group.MinSize != tt.wantGroup.MinSize {
				t.Errorf("groupTypeFromString() = %#v, want %#v", group, tt.wantGroup)
			}

			groupRecord := group.NewGroup()

			for _, record := range tt.records {
				groupRecord.Add(record)
			}

			groupRecord.Merge()

			wantRecords := [][]*Record{}
			for records := range groupRecord.Iter() {
				wantRecords = append(wantRecords, records)
			}

			sort.Slice(wantRecords, func(i, j int) bool {
				if group.Type == groupTypeKey {
					return bytes.Compare(wantRecords[i][0].Key, wantRecords[j][0].Key) < 0
				}

				if group.Type == groupTypePartition {
					return wantRecords[i][0].Partition < wantRecords[j][0].Partition
				}

				return bytes.Compare(wantRecords[i][0].Value, wantRecords[j][0].Value) < 0
			})

			for i, record := range wantRecords {
				if len(record) != len(tt.wantRecords[i]) {
					t.Fatalf("groupRecord = %d records, want %d", len(record), len(tt.wantRecords[i]))
				}

				for j, r := range record {
					if !bytes.Equal(r.Key, tt.wantRecords[i][j].Key) || !bytes.Equal(r.Value, tt.wantRecords[i][j].Value) {
						t.Errorf("groupRecord.Add() = %s:%s, want %s:%s", r.Key, r.Value, tt.wantRecords[i][j].Key, tt.wantRecords[i][j].Value)
					}
				}
			}
		})
	}
}

func Test_mergeGroup(t *testing.T) {
	type args struct {
		min int
		m   map[int][]*Record
	}
	tests := []struct {
		name    string
		args    args
		want    map[int][]*Record
		skipKey bool
	}{
		{
			name: "no short lists, nothing to merge",
			args: args{
				min: 2,
				m: map[int][]*Record{
					1: {{Key: []byte("a")}, {Key: []byte("b")}},
					2: {{Key: []byte("c")}, {Key: []byte("d")}},
				},
			},
			want: map[int][]*Record{
				1: {{Key: []byte("a")}, {Key: []byte("b")}},
				2: {{Key: []byte("c")}, {Key: []byte("d")}},
			},
		},
		{
			name: "one short list merged into valid target",
			args: args{
				min: 2,
				m: map[int][]*Record{
					1: {{Key: []byte("a")}},
					2: {{Key: []byte("b")}, {Key: []byte("c")}},
				},
			},
			want: map[int][]*Record{
				2: {{Key: []byte("b")}, {Key: []byte("c")}, {Key: []byte("a")}},
			},
		},
		{
			name: "multiple short lists, no valid targets, merge into longest",
			args: args{
				min: 3,
				m: map[int][]*Record{
					1: {{Key: []byte("a")}},
					2: {{Key: []byte("b")}, {Key: []byte("c")}},
					3: {{Key: []byte("d")}},
				},
			},
			want: func() map[int][]*Record {
				// After merge, all records should be in the longest list (key 2)
				return map[int][]*Record{
					2: {{Key: []byte("b")}, {Key: []byte("c")}, {Key: []byte("a")}, {Key: []byte("d")}},
				}
			}(),
		},
		{
			name: "all lists are short, merge into one",
			args: args{
				min: 5,
				m: map[int][]*Record{
					1: {{Key: []byte("a")}},
					2: {{Key: []byte("b")}},
					3: {{Key: []byte("c")}},
				},
			},
			want: func() map[int][]*Record {
				// All records should be merged into the longest list (any, since all are length 1)
				return map[int][]*Record{
					1: {{Key: []byte("a")}, {Key: []byte("b")}, {Key: []byte("c")}},
				}
			}(),
			skipKey: true, // Skip key check since all are length 1
		},
		{
			name: "empty map, nothing to merge",
			args: args{
				min: 2,
				m:   map[int][]*Record{},
			},
			want: map[int][]*Record{},
		},
		{
			name: "one list only, less than min",
			args: args{
				min: 3,
				m: map[int][]*Record{
					1: {{Key: []byte("a")}},
				},
			},
			want: map[int][]*Record{
				1: {{Key: []byte("a")}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mergeGroup(tt.args.min, tt.args.m)
			if len(tt.args.m) != len(tt.want) {
				t.Fatalf("mergeGroup() got = %v, want %v", tt.args.m, tt.want)
			}

			totalLength := 0
			for _, records := range tt.args.m {
				totalLength += len(records)
			}
			for _, records := range tt.want {
				totalLength -= len(records)
			}

			if totalLength != 0 {
				t.Errorf("mergeGroup() total length mismatch, got %d, want 0", totalLength)
			}

			if tt.skipKey {
				return // Skip key check for this test case
			}

			for k, v := range tt.want {
				if got, ok := tt.args.m[k]; !ok || len(got) != len(v) {
					t.Errorf("mergeGroup() missing or wrong length for key %d, got %v, want %v", k, got, v)
					continue
				}
			}
		})
	}
}

func TestMixCount(t *testing.T) {
	groupType, err := groupTypeFromString("mix")
	if err != nil {
		t.Fatalf("failed to get group type: %v", err)
	}

	if groupType != groupTypeMix {
		t.Errorf("unexpected group type: got %v, want %v", groupType, groupTypeMix)
	}

	group := &group{
		Type:      groupType,
		BatchSize: 2000,
		MinSize:   1,
		RunSize:   2000,
	}

	groupMix := group.NewGroup()

	total := atomic.Int64{}
	for i := 0; i < 1500; i++ {
		total.Add(int64(i + 1))
		groupMix.Add(&Record{Key: []byte(fmt.Sprintf("%d", i+1)), Value: []byte(fmt.Sprintf("value%d", i))})
	}

	ok := groupMix.IsEnough()
	if ok {
		t.Errorf("IsEnough() = %v, want %v", ok, true)
	}

	errGroup, _ := errgroup.WithContext(t.Context())
	errGroup.SetLimit(10)

	groupMix.Merge()
	for records := range groupMix.Iter() {
		if groupMix.IsSingle() {
			for _, record := range records {
				errGroup.Go(func() error {
					v, err := strconv.ParseInt(string(record.Key), 10, 64)
					if err != nil {
						t.Fatal(err)
					}

					total.Add(-v)
					return nil
				})
			}
		} else {
			errGroup.Go(func() error {
				for _, record := range records {
					t.Fatal(record)
				}

				return nil
			})
		}
	}

	if err := errGroup.Wait(); err != nil {
		t.Errorf("wait group failed: %v", err)
	}

	if total.Load() != 0 {
		t.Errorf("total = %d, want 0", total.Load())
	}
}
