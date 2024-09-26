package main

import (
	"context"
	"log/slog"
	"os"
	"sort"

	"github.com/rakunlabs/into"
	"github.com/rakunlabs/logi"

	"github.com/worldline-go/wkafka/example/admin"
	"github.com/worldline-go/wkafka/example/consumer"
	"github.com/worldline-go/wkafka/example/producer"
)

var examples = map[string]func(context.Context) error{
	"admin_topic":             admin.RunExampleTopic,
	"admin_partition":         admin.RunExamplePartition,
	"admin_list":              admin.RunExampleList,
	"consumer_batch":          consumer.RunExampleBatch,
	"consumer_batch_err":      consumer.RunExampleBatchErr,
	"consumer_single":         consumer.RunExampleSingle,
	"consumer_single_handler": consumer.RunExampleSingleWithHandler,
	"consumer_single_byte":    consumer.RunExampleSingleByte,
	"producer_hook":           producer.RunExampleHook,
}

func getExampleList() []string {
	exampleNames := make([]string, 0, len(examples))
	for k := range examples {
		exampleNames = append(exampleNames, k)
	}

	sort.Strings(exampleNames)

	return exampleNames
}

func main() {
	exampleName := os.Getenv("EXAMPLE")

	if exampleName == "" {
		slog.Error("EXAMPLE env variable is not set", slog.Any("examples", getExampleList()))

		return
	}

	run := examples[exampleName]
	if run == nil {
		slog.Error("unknown example", slog.String("example", exampleName))

		return
	}

	into.Init(
		run,
		into.WithLogger(logi.InitializeLog(logi.WithCaller(false))),
	)
}
