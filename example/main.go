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
	"github.com/worldline-go/wkafka/example/demo"
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
	"consumer_single_trace":   consumer.RunExampleSingleWithTrace,
	"producer_hook":           producer.RunExampleHook,

	"demo_admin": demo.RunAdmin,

	"demo_push":                        demo.RunPush,
	"demo_consume_single":              demo.RunConsumeSingle,
	"demo_consume_batch":               demo.RunConsumeBatch,
	"demo_consume_concurrent_single":   demo.RunConsumeConcurrentSingle,
	"demo_consume_concurrent_batch":    demo.RunConsumeConcurrentBatch,
	"demo_consume_single_block":        demo.RunConsumeSingleBlock,
	"demo_consume_single_error":        demo.RunConsumeSingleError,
	"demo_consume_single_error_dlq":    demo.RunConsumeSingleErrorDLQ,
	"demo_consume_single_error_dlq_ui": demo.RunConsumeSingleErrorDLQUI,
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
		into.WithMsgf("EXAMPLE=%s", exampleName),
		into.WithLogger(logi.InitializeLog(logi.WithCaller(false))),
	)
}
