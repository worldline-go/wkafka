package wkafka

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
)

func Test_producerDLQ(t *testing.T) {
	clientID := []byte("test-client")
	topic := "test-topic"

	errDLQ := &DLQError{
		Err: errors.New("test error"),
		Indexes: map[int]error{
			0: errors.New("indexed error"),
			2: nil,
		},
	}

	orgRecords := []*kgo.Record{
		{
			Topic:     "original-topic",
			Key:       []byte("key"),
			Value:     []byte("value"),
			Headers:   []kgo.RecordHeader{},
			Offset:    123,
			Partition: 1,
			Timestamp: time.Now(),
		},
		{
			// This record should be skipped
			Topic:     "original-topic",
			Key:       []byte("key"),
			Value:     []byte("value-1"),
			Headers:   []kgo.RecordHeader{},
			Offset:    124,
			Partition: 1,
			Timestamp: time.Now(),
		},
		{
			Topic:     "original-topic",
			Key:       []byte("key"),
			Value:     []byte("value-2"),
			Headers:   []kgo.RecordHeader{},
			Offset:    125,
			Partition: 2,
			Timestamp: time.Now(),
		},
	}

	// Define a mock function to be wrapped by producerDLQ
	mockFn := func(ctx context.Context, records []*kgo.Record) error {
		for i := range records {
			var orgRecord *kgo.Record
			errDLQStr := errDLQ.Err.Error()
			switch i {
			case 0:
				orgRecord = orgRecords[0]
				errDLQStr = "indexed error"
			case 1:
				orgRecord = orgRecords[2]
			}

			for _, header := range records[i].Headers {
				switch header.Key {
				case defaultServiceNameKey:
					assert.Equal(t, clientID, header.Value)
				case "error":
					assert.Equal(t, errDLQStr, string(header.Value))
				case "offset":
					assert.Equal(t, strconv.FormatInt(orgRecord.Offset, 10), string(header.Value))
				case "partition":
					assert.Equal(t, strconv.FormatInt(int64(orgRecord.Partition), 10), string(header.Value))
				case "topic":
					assert.Equal(t, orgRecord.Topic, string(header.Value))
				case "timestamp":
					assert.Equal(t, orgRecord.Timestamp.Format(time.RFC3339), string(header.Value))
				}
			}
		}

		return nil
	}

	dlqProducer := producerDLQ(topic, clientID, mockFn)

	err := dlqProducer(context.Background(), errDLQ, orgRecords)
	assert.NoError(t, err)
}
