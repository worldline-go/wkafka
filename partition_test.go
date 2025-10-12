package wkafka

import (
	"slices"
	"testing"
)

func Test_partitionHandler_IsRevokedRecordBatch(t *testing.T) {
	tests := []struct {
		name    string // description of this test case
		revoked map[string][]int32
		records []*Record
		want    []int
		want2   bool
	}{
		{
			name:    "no revoked partitions",
			revoked: map[string][]int32{},
			records: []*Record{
				{Topic: "topic1", Partition: 0},
				{Topic: "topic1", Partition: 1},
				{Topic: "topic2", Partition: 0},
			},
			want:  []int{0, 1, 2},
			want2: false,
		},
		{
			name: "some revoked partitions",
			revoked: map[string][]int32{
				"topic1": {1},
			},
			records: []*Record{
				{Topic: "topic1", Partition: 0},
				{Topic: "topic1", Partition: 1},
				{Topic: "topic2", Partition: 0},
			},
			want:  []int{0, 2},
			want2: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var h partitionHandler
			h.AddPartitionsRevoked(tt.revoked)
			got, got2 := h.IsRevokedRecordBatch(tt.records)

			wantRecords := make([]*Record, 0, len(tt.want))
			for _, idx := range tt.want {
				wantRecords = append(wantRecords, tt.records[idx])
			}

			if !slices.Equal(got, wantRecords) {
				t.Errorf("IsRevokedRecordBatch() got = %v, want %v", got, wantRecords)
			}

			if got2 != tt.want2 {
				t.Errorf("IsRevokedRecordBatch() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func Test_partitionHandler_IsRevokedRecord(t *testing.T) {
	tests := []struct {
		name    string // description of this test case
		revoked map[string][]int32
		r       *Record
		want    bool
	}{
		{
			name:    "no revoked partitions",
			revoked: map[string][]int32{},
			r:       &Record{Topic: "topic1", Partition: 0},
			want:    false,
		},
		{
			name: "partition not revoked",
			revoked: map[string][]int32{
				"topic1": {1},
			},
			r:    &Record{Topic: "topic1", Partition: 0},
			want: false,
		},
		{
			name: "partition not revoked different topic",
			revoked: map[string][]int32{
				"topic1": {1},
			},
			r:    &Record{Topic: "topic2", Partition: 0},
			want: false,
		},
		{
			name: "partition revoked",
			revoked: map[string][]int32{
				"topic1": {1},
			},
			r:    &Record{Topic: "topic1", Partition: 1},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var h partitionHandler
			h.AddPartitionsRevoked(tt.revoked)

			got := h.IsRevokedRecord(tt.r)
			if got != tt.want {
				t.Errorf("IsRevokedRecord() = %v, want %v", got, tt.want)
			}
		})
	}
}
