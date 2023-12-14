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
Main config struct that contains brokers and security settings.

```yaml
brokers:
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
```

### Consumer

For creating a consumer we need to give config while creating a client with a processor struct.

### Producer

Use consumer client or create without consumer settings, `NewClient` also try to connect to brokers.

```go
client, err := wkafka.NewClient(kafkaConfig)
if err != nil {
    return err
}
defer client.Close()
```

Create a producer based of client to set default values.

> TODO add example

## Development

Initialize kafka and redpanda console with docker-compose

```sh
make env
```

| Service        | Description      |
| -------------- | ---------------- |
| localhost:9092 | Kafka broker     |
| localhost:7071 | Redpanda console |
