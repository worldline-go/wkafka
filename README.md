# wkafka

[![License](https://img.shields.io/github/license/worldline-go/wkafka?color=red&style=flat-square)](https://raw.githubusercontent.com/worldline-go/wkafka/main/LICENSE)
[![Coverage](https://img.shields.io/sonar/coverage/worldline-go_wkafka?logo=sonarcloud&server=https%3A%2F%2Fsonarcloud.io&style=flat-square)](https://sonarcloud.io/summary/overall?id=worldline-go_wkafka)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/worldline-go/wkafka/test.yml?branch=main&logo=github&style=flat-square&label=ci)](https://github.com/worldline-go/wkafka/actions)
[![Go PKG](https://raw.githubusercontent.com/worldline-go/guide/main/badge/custom/reference.svg)](https://pkg.go.dev/github.com/worldline-go/wkafka)

wkafka is a wrapper for kafka library to initialize and use for microservices.

```sh
go get github.com/worldline-go/wkafka
```

This library is using [franz-go](https://github.com/twmb/franz-go).

Requires Go 1.27 or later.

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

### OAuth authentication

Enable at most one mechanism (`plain`, `scram`, or `oauth`) per SASL list entry.
Separate entries can advertise different mechanisms in preference order.
`SaslConfig` is an alias for the existing `SalsConfig` Go type.

For OAuth client credentials:

```yaml
security:
  tls:
    enabled: true
  sasl:
    - oauth:
        enabled: true
        token_url: https://identity.example.com/realms/apps/protocol/openid-connect/token
        client_id: kafka-worker
        client_secret: your-secret
        scopes: [kafka]
        timeout: 10s
        # proxy: http://proxy.example.com:3128  # token endpoint proxy, default: HTTPS_PROXY / NO_PROXY env
        zid: ""
        extensions: {}
```

Tokens are fetched lazily during authentication, cached per Kafka client, and
refreshed near expiry using the token endpoint's `expires_in`. Requests use the
current authentication context and a timeout (default 10 seconds). The broker
must support OAUTHBEARER and trust the issuing identity provider.

Alternatively, set `token` for a static bearer token, or supply a Go callback:

```go
cfg.Security.SASL = wkafka.SaslConfigs{{
    OAuth: wkafka.SaslOAuth{
        Enabled: true,
        TokenProvider: func(ctx context.Context) (oauth.Auth, error) {
            token, err := tokenProvider(ctx) // Your token acquisition/renewal function.
            return oauth.Auth{Token: token}, err
        },
    },
}}
```

The callback uses `github.com/twmb/franz-go/pkg/sasl/oauth`. It must be safe for
concurrent calls and manage caching/renewal itself. Its returned `Zid` and
`Extensions` are used as-is. Configure exactly one of static token, client
credentials, `gcp`, or callback; static tokens are not renewed.

#### Client credentials with mutual TLS

Set `tls` to present a client certificate to the token endpoint (RFC 8705).
With a client certificate, `client_secret` is optional: when it is empty,
`client_id` is sent in the request body and the certificate authenticates the
client (`tls_client_auth`). When it is set, both are used.

```yaml
security:
  tls:
    enabled: true
  sasl:
    - oauth:
        enabled: true
        token_url: https://identity.example.com/realms/apps/protocol/openid-connect/token
        client_id: kafka-worker
        scopes: [kafka]
        tls:
          cert_file: /etc/certs/oauth/client-cert.pem
          key_file: /etc/certs/oauth/client-key.pem
          ca_file: /etc/certs/oauth/ca.pem   # optional, system roots otherwise
```

`oauth.tls` only applies to the token endpoint. Broker TLS is still configured
under `security.tls`.

#### Google Cloud workload identity federation (X.509)

`gcp` gets Google Cloud access tokens with an X.509 client certificate
(Workload Identity Federation over mTLS). If configured, it then impersonates
a service account. Use the `external_account` credential file from Google;
its `credential_source` is ignored, because `cert_file` / `key_file` are used
instead.

```yaml
security:
  tls:
    enabled: true
    cert_file: /etc/certs/kafka/client-cert.pem
    key_file: /etc/certs/kafka/client-key.pem
    ca_file: /etc/certs/kafka/ca.pem
  sasl:
    - oauth:
        enabled: true
        timeout: 10s
        gcp:
          enabled: true
          credentials_file: /etc/certs/gcp/wif-credentials.json
          cert_file: /etc/certs/kafka/client-cert.pem
          key_file: /etc/certs/kafka/client-key.pem
          format: raw            # managed_kafka for Google Managed Service for Apache Kafka
          # proxy: http://proxy.example.com:3128  # default: oauth.proxy, then HTTPS_PROXY / NO_PROXY env
          # lifetime: 1h         # impersonated token lifetime
          # refresh_before: 5m   # refresh this long before expiry
          # scopes: [https://www.googleapis.com/auth/cloud-platform]
```

Example `wif-credentials.json` (placeholder values):

```json
{
  "type": "external_account",
  "audience": "//iam.googleapis.com/projects/000000000000/locations/global/workloadIdentityPools/my-pool/providers/my-provider",
  "subject_token_type": "urn:ietf:params:oauth:token-type:mtls",
  "token_url": "https://sts.mtls.googleapis.com/v1/token",
  "credential_source": { "certificate": { "use_default_certificate_config": true } },
  "service_account_impersonation_url": "https://iamcredentials.mtls.googleapis.com/v1/projects/-/serviceAccounts/my-sa@my-project.iam.gserviceaccount.com:generateAccessToken"
}
```

`audience`, `token_url` and `service_account_impersonation_url` can also be
set directly and override the credential file. Use `format: managed_kafka` for
Google Managed Service for Apache Kafka, which expects the token wrapped in a
`GOOG_OAUTH2_TOKEN` JWT-like envelope. Other brokers usually expect `raw`.
If a refresh fails, the cached token keeps being used until it actually expires.

#### Inline values and certificate rotation

In both `oauth.tls` and `oauth.gcp`, each of `cert`, `key`, `ca` (and
`credentials` for `gcp`) can be given inline as PEM/JSON content instead of
with the `_file` form. You cannot set both forms of the same value. Certificate
files are re-read on every token refresh, so rotated certificates (for
example from cert-manager) are picked up without a restart.

Security config applies to both the main and DLQ clients, including when using
`WithKGOOptionsDLQ`. TLS client certificates require both `cert_file` and `key_file`.

#### Disabling server certificate verification

`insecure_skip_verify` defaults to `false`. Set it to `true` under
`security.tls` (Kafka, including DLQ), `oauth.tls` (client credentials token
endpoint), `oauth.gcp` (both Google STS and service account impersonation
endpoints), or the Redis handler's `tls` configuration. Kafka and Redis still
require `enabled: true` to enable TLS.

```yaml
security:
  tls:
    enabled: true
    insecure_skip_verify: true
```

**Warning:** This disables server certificate chain and hostname verification,
making the connection vulnerable to man-in-the-middle attacks. Prefer configuring
`ca_file` (or inline `ca` for OAuth/GCP); do not use this option in production.

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
# settings for default decoder (JSON)
decoder:
  skip_invalid: true # skip invalid messages that can't be decoded; default is true
# flag to block rebalance while pulled messages on process
# use with block_rebalance_timeout and max_poll_records
block_rebalance: false
# only works with block_rebalance option
# DLQ consumer does not use block rebalance
block_rebalance_timeout: 60s # timeout to block rebalance, default is 60 seconds
# max records to consume per batch to give callback function, default is 100
# caps ordinary and concurrent mix batches; key/partition groups can exceed this size
batch_count: 100
concurrent:
  enabled: false # enable concurrent processing of messages
  process: 10 # max simultaneous processing; 1 keeps grouping, nonpositive values default to 10
  min_size: 1 # minimum size of the bucket for merging multiple bucket, default is 1
  run_size: 0 # records per run; nonpositive uses batch_count * process for concurrent mix batches, batch_count otherwise
  type: "key" # type of grouping records to process, can be "mix", "partition", "key"; default is "key"
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

With `concurrent.enabled: true`, both single and batch consumers use the configured grouping even when `process: 1`.
`process` only limits simultaneous work: setting it to 1 processes groups serially, rather than switching to ordinary ungrouped processing.
For `key` and `partition`, records stay in their groups, subject to `min_size` merging; `batch_count` is not an upper limit for these groups.

For __WithCallbackBatch__ with concurrent `mix` grouping, an unset or nonpositive `run_size` defaults to `batch_count * process` so a full run can supply one batch per worker.
With `process: 1`, this is simply `batch_count`. An explicit positive `run_size` is always preserved.
Single consumers, key/partition grouping, and disabled concurrency keep the `batch_count` default.

```go
// example single consumer
if err := client.ConsumeSingle(ctx, ProcessSingle); err != nil {
  return fmt.Errorf("consume: %w", err)
}
```

For batches, use `client.ConsumeBatch(ctx, ProcessBatch)`, where the callback accepts
`[]T` instead of `T`. Both methods infer `T` from the callback and accept the same
options as `Consume`, including `WithDecode[T]`. Default decoding is JSON; use
`T = []byte` for raw messages (`[][]byte` in a batch callback).
The existing `Consume` API with `WithCallback` or `WithCallbackBatch` remains available.

Send record to dead letter queue, use __WrapErrDLQ__ function with to wrap the error and it will be send to dead letter queue.

> Check the aditional options for custom decode and precheck.

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
producer, err := client.NewProducer[*Data]("test", wkafka.WithHook(ProduceHook))
if err != nil {
  return err
}

return producer.Produce(ctx, data)
```

### Plugins

#### Handler

```go
import (
  "github.com/worldline-go/wkafka/handler"
)
```

Editing the skip map and use our handler to initialize server mux.

> `Redis` like API with support channel need for request goes to other instances.

In the kafka config add the `plugins` section with `handler` plugin.

```yaml
plugins:
  handler:
    enabled: true # enable plugins
    addr: ":17070" # address to listen, default is ":17070"
    pubsub:
      prefix: "finops_", # prefix for pubsub channels, default is empty
      redis:
        address: "localhost:6379" # address for redis like API
```

Than enable the handler in the kafka client initialization.

```go
wkafka.WithPlugin(handler.PluginWithName()),
```

Or you can do it manually:

```go
// import github.com/worldline-go/wkafka/handler

mux := http.NewServeMux()
mux.Handle(handler.New(ctx, client, handler.Config{}.ToOption()))
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

| Service                         | Description              |
|---------------------------------|--------------------------|
| localhost:9092                  | Kafka broker for connect |
| http://localhost:7071           | Redpanda console         |
| http://localhost:3000           | Grafana                  |
| http://localhost:8080/wkafka/ui | Handler UI               |

Use examples with `EXAMPLE` env variable:

```sh
EXAMPLE=... make example
```
