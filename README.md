# wkafka

[![License](https://img.shields.io/github/license/worldline-go/wkafka?color=red&style=flat-square)](https://raw.githubusercontent.com/worldline-go/wkafka/main/LICENSE)
[![Coverage](https://img.shields.io/sonar/coverage/worldline-go_wkafka?logo=sonarcloud&server=https%3A%2F%2Fsonarcloud.io&style=flat-square)](https://sonarcloud.io/summary/overall?id=worldline-go_wkafka)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/worldline-go/wkafka/test.yml?branch=main&logo=github&style=flat-square&label=ci)](https://github.com/worldline-go/wkafka/actions)
[![Go Report Card](https://goreportcard.com/badge/github.com/worldline-go/wkafka?style=flat-square)](https://goreportcard.com/report/github.com/worldline-go/wkafka)
[![Go PKG](https://raw.githubusercontent.com/worldline-go/guide/main/badge/custom/reference.svg)](https://pkg.go.dev/github.com/worldline-go/wkafka)

wkafka is a wrapper for kafka library to initialize and use for microservices.

```sh
go get github.com/worldline-go/wkafka
```

This library is using [franz-go](https://github.com/twmb/franz-go).

## Usage

First set the connection config to create a new kafka client.  
Main config struct that contains brokers, security settings and consumer validation.

```yaml
brokers: # list of brokers, default is empty
  - localhost:9092
security:
  tls:
    enabled: false
    cert_file: ""
    key_file: ""
    ca_file: ""
  sasl: # SASL/SCRAM authentication could be multiple and will be used in order
    - plain:
        enabled: false
        user: ""
        pass: ""
      scram:
        enabled: false
        algorithm: "" # "SCRAM-SHA-256" or "SCRAM-SHA-512"
        user: ""
        pass: ""
consumer: # consumer validation and default values
  prefix_group_id: "" # add always a prefix to group id
  format_dlq_topic: "" # format dead letter topic name, ex: "finops_{{.AppName}}_dlq"
  validation:
    group_id: # validate group id
      enabled: false
      rgx_group_id: "" # regex to validate group id ex: "^finops_.*$"
plugins: # additional configuration for plugins
  plugin_name: # plugin name
    key: value # plugin's key value configuration
```

### Consumer

For creating a consumer we need to give additional consumer config when initializing the client.

```yaml
topics: [] # list of topics to subscribe
group_id: "" # group id to subscribe, make is as unique as possible per service
# start offset to consume, 0 is the earliest offset, -1 is the latest offset and more than 0 is the offset number
# group_id has already committed offset then this will be ignored
start_offset: 0 # -1 to start end of the offsets
skip: # this is programatically skip, kafka will still consume the message
  # example skip topic and offset
  mytopic: # topic name to skip
    0: # partition number
      offsets: # list of offsets to skip
        - 31
        - 90
      before: 20 # skip all offsets before or equal to this offset
# max records to consume per poll, 0 is default value which is not checking
# use this option with block_rebalance
max_poll_records: 0
# flag to block rebalance while pulled messages on process
# use with block_rebalance_timeout and max_poll_records
block_rebalance: false
# only works with block_rebalance option
# DLQ consumer does not use block rebalance
block_rebalance_timeout: 60s # timeout to block rebalance, default is 60 seconds
# max records to consume per batch to give callback function, default is 100
# if this value is more than max_poll_records then max_poll_records will be used
batch_count: 100
dlq:
  disabled: false # disable dead letter queue
  topic: "" # dead letter topic name, it can be assigned in the kafka config's format_dlq_topic
  retry_interval: "10s" # retry time interval of the message if can't be processed, default is 10s
  retry_max_interval: "15m" # max interval for exponential time duration limit, default is 15m
  start_offset: 0 # -1 to start end of the offsets
  skip: # same as skip but just for dead letter topic and not need to specify topic name
    # example skip offset
    0:
      offsets:
        - 31
      before: 20
```

Always give the client information so we can view in publish message's headers and kafka UI.

```go
client, err := wkafka.New(
  ctx, kafkaConfig,
  wkafka.WithConsumer(consumeConfig),
  wkafka.WithClientInfo("testapp", "v0.1.0"),
)
if err != nil {
  return err
}

defer client.Close()
```

Now you need to run consumer with a callback function.  
There is 2 options to run consumer, batch or single (__WithCallbackBatch__ or __WithCallback__).  
Default decoder is json, but you can change it with __WithDecode__ option.  
If you use `[]byte` as data type then raw data will be passed to the callback function, batch consumer like `[][]byte` type.

```go
// example single consumer
if err := client.Consume(ctx, wkafka.WithCallback(ProcessSingle)); err != nil {
  return fmt.Errorf("consume: %w", err)
}
```

Send record to dead letter queue, use __WrapErrDLQ__ function with to wrap the error and it will be send to dead letter queue.

> Check the aditional options for custom decode and precheck.


#### Skip Handler

Editing the skip map and use our handler to initialize server mux.

> `Redis` need for request goes to other instances.

```go
// import github.com/worldline-go/wkafka/handler

mux := http.NewServeMux()
mux.Handle(handler.New(ctx, client))
```

It will serve default on `/wkafka/ui` path.

<details><summary>Handler Example</summary>

```sh
make env

# run the example
EXAMPLE=consumer_single_handler make example
```

Add messages in here to skip the message http://localhost:7071

</details>

### Producer

Use consumer client or create without consumer settings, `New` also try to connect to brokers.

```go
client, err := wkafka.New(kafkaConfig)
if err != nil {
    return err
}
defer client.Close()
```

Create a producer based of client and specific data type.

> __WithHook__, __WithEncoder__, __WithHeaders__ options are optional.  
> Use __WithHook__ to get metadata of the record and modify to produce record.

```go
producer, err := wkafka.NewProducer[*Data](client, "test", wkafka.WithHook(ProduceHook))
if err != nil {
  return err
}

return producer.Produce(ctx, data)
```

### Telemetry

```sh
go get github.com/twmb/franz-go/plugin/kotel
```

Use that with initializing the kafka client.

```go
kafkaTracer := kotel.NewTracer()

kafkaClient, err = wkafka.New(ctx,
  config.Application.KafkaConfig,
  wkafka.WithConsumer(config.Application.KafkaConsumer),
  wkafka.WithClientInfo(config.ServiceName, config.ServiceVersion),
  wkafka.WithKGOOptions(kgo.WithHooks(kotel.NewKotel(kotel.WithTracer(kafkaTracer)).Hooks()...)),
)
```

#### Telemetry on Produce Message

Important to have span kind as producer.

```go
ctx, spanKafka := otel.Tracer("").Start(ctx, "produce_message", trace.WithSpanKind(trace.SpanKindProducer))
defer spanKafka.End()

if err := h.KafkaProducer.Produce(ctx, product); err != nil {
    spanKafka.SetStatus(codes.Error, err.Error())

    return c.JSON(http.StatusBadRequest, model.Message{
        Message: err.Error(),
    })
}
```

#### Telemetry on Consume Message

`k.Tracer` is we initialized on kafka client (`kotel.NewTracer()`).

```go
func (k *Kafka) Consume(ctx context.Context, product model.Product) error {
	// use tracer's returned ctx for next spans
	ctx, span := k.Tracer.WithProcessSpan(wkafka.CtxRecord(ctx))
	defer span.End()

	span.SetAttributes(attribute.String("product.name", product.Name))

	log.Info().Str("product", product.Name).Str("description", product.Description).Msg("consume message")

	return nil
}
```

## Development

Initialize kafka and redpanda console with docker-compose.

```sh
# using "docker compose" command, if you use podman then add compose extension and link docker with podman binary
make env
```

| Service        | Description      |
| -------------- | ---------------- |
| localhost:9092 | Kafka broker     |
| localhost:7071 | Redpanda console |

Use examples with `EXAMPLE` env variable:

```sh
EXAMPLE=... make example
```
