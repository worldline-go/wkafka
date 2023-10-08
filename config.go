package wkafka

type Config struct {
	// Brokers is a list of kafka brokers to connect to.
	// Not all brokers need to be specified, the list is so that
	// if one broker is unavailable, another can be used.
	// Required at least one broker. Default is "localhost:9092" for local development.
	Brokers  []string       `cfg:"brokers"  default:"localhost:9092"`
	Security SecurityConfig `cfg:"security"`
}
