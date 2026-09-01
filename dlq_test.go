package wkafka

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptionDLQTriggerToOptionCopiesSpecPartitions(t *testing.T) {
	partitions := map[string][]int32{"topic": {1, 2}}
	input := OptionDLQTrigger{SpecPartitions: partitions}
	output := OptionDLQTrigger{}

	input.ToOption()(&output)

	require.Equal(t, partitions, output.SpecPartitions)
}
