package wkafka

import (
	"fmt"
	"regexp"
)

type Config struct {
	// Brokers is a list of kafka brokers to connect to.
	// Not all brokers need to be specified, the list is so that
	// if one broker is unavailable, another can be used.
	// Required at least one broker. Default is "localhost:9092" for local development.
	Brokers  []string       `cfg:"brokers"  default:"localhost:9092"`
	Security SecurityConfig `cfg:"security"`
	// Compressions is chosen in the order preferred based on broker support.
	// The default is to use no compression.
	//  Available:
	//  - gzip
	//  - snappy
	//  - lz4
	//  - zstd
	Compressions []string `cfg:"compressions"`

	Consumer ConsumerPreConfig `cfg:"consumer"`
}

type ConsumerPreConfig struct {
	// PrefixGroupID add prefix to group_id.
	PrefixGroupID string `cfg:"prefix_group_id"`
	// FormatDLQTopic is a format string to generate DLQ topic name.
	//  - %s is a placeholder for program name.
	//  - Default is "finops_%s_dlq"
	FormatDLQTopic string `cfg:"format_dlq_topic"`
	// Validation is a configuration for validation when consumer initialized.
	Validation Validation `cfg:"validation"`
}

// Apply configuration to ConsumerConfig and check validation.
func (c ConsumerPreConfig) Apply(consumerConfig ConsumerConfig, progName string) (ConsumerConfig, error) {
	if c.PrefixGroupID != "" {
		consumerConfig.GroupID = c.PrefixGroupID + consumerConfig.GroupID
	}

	// add default topic name for DLQ
	if !consumerConfig.DLQ.Disable {
		if consumerConfig.DLQ.Topic == "" {
			if c.FormatDLQTopic == "" {
				c.FormatDLQTopic = "finops_%s_dlq"
			}

			consumerConfig.DLQ.Topic = fmt.Sprintf(c.FormatDLQTopic, progName)
		}

		if consumerConfig.DLQ.SkipExtra == nil {
			consumerConfig.DLQ.SkipExtra = map[string]map[int32]Offsets{
				consumerConfig.DLQ.Topic: consumerConfig.DLQ.Skip,
			}
		} else {
			consumerConfig.DLQ.SkipExtra[consumerConfig.DLQ.Topic] = consumerConfig.DLQ.Skip
		}
	}

	if err := c.Validation.Validate(consumerConfig); err != nil {
		return consumerConfig, fmt.Errorf("validate consumer config: %w", err)
	}

	return consumerConfig, nil
}

// Validation is a configuration for validation when consumer initialized.
type Validation struct {
	GroupID GroupIDValidation `cfg:"group_id"`
}

// GroupIDValidation is a configuration for group_id validation.
type GroupIDValidation struct {
	Enabled bool `cfg:"enabled"`
	// RgxGroupID is a regex pattern to validate RgxGroupID.
	RgxGroupID string `cfg:"rgx_group_id"`
	// DisableWord boundary check.
	DisableWordBoundary bool `cfg:"disable_word_boundary"`
}

func (v GroupIDValidation) Validate(groupID string) error {
	if !v.Enabled {
		return nil
	}

	if groupID == "" {
		return fmt.Errorf("group_id is required")
	}

	if v.RgxGroupID != "" {
		if !v.DisableWordBoundary {
			v.RgxGroupID = fmt.Sprintf(`\b%s\b`, v.RgxGroupID)
		}

		rgx, err := regexp.Compile(v.RgxGroupID)
		if err != nil {
			return fmt.Errorf("group_id validation regex: %w", err)
		}

		if !rgx.MatchString(groupID) {
			return fmt.Errorf("group_id validation failed regex [%s], value [%s]", v.RgxGroupID, groupID)
		}
	}

	return nil
}

func (v Validation) Validate(consumerConfig ConsumerConfig) error {
	if err := v.GroupID.Validate(consumerConfig.GroupID); err != nil {
		return err
	}

	return nil
}
