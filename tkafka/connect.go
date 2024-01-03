package tkafka

import (
	"context"
	"os"
	"strings"

	"github.com/worldline-go/wkafka"
)

var DefaultBrokerAddress = []string{"127.0.0.1:9092"}

func BrokerAddress() []string {
	brokers := DefaultBrokerAddress

	add := os.Getenv("KAFKA_BROKER")
	if add != "" {
		brokers = strings.Fields(strings.ReplaceAll(add, ",", " "))
	}

	return brokers
}

// Config returns the default configuration for Kafka for testing.
func Config() wkafka.Config {
	return wkafka.Config{
		Brokers: BrokerAddress(),
	}
}

func TestClient() (*wkafka.Client, error) {
	return wkafka.New(context.Background(), Config())
}
