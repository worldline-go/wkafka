package wkafka

import (
	"fmt"
	"regexp"
	"time"
)

var (
	DefaultRetryInterval    = 10 * time.Second
	DefaultRetryMaxInterval = 15 * time.Minute
)

type Config struct {
	// Brokers is a list of kafka brokers to connect to.
	// Not all brokers need to be specified, the list is so that
	// if one broker is unavailable, another can be used.
	// Required at least one broker. Example value is 'localhost:9092'.
	Brokers  []string       `cfg:"brokers"  json:"brokers"`
	Security SecurityConfig `cfg:"security" json:"security"`
	// Compressions is chosen in the order preferred based on broker support.
	// The default is to use no compression.
	//  Available:
	//  - gzip
	//  - snappy
	//  - lz4
	//  - zstd
	Compressions []string `cfg:"compressions" json:"compressions"`

	// Consumer is a pre configuration for consumer and validation.
	Consumer ConsumerPreConfig `cfg:"consumer" json:"consumer"`

	// Plugins add custom plugins to the client like handler.
	Plugins map[string]interface{} `cfg:"plugins" json:"plugins"`
}

type ConsumerPreConfig struct {
	// PrefixGroupID add prefix to group_id.
	PrefixGroupID string `cfg:"prefix_group_id" json:"prefix_group_id"`
	// FormatDLQTopic is a format string to generate DLQ topic name.
	//  - Example is "finops_{{.AppName}}_dlq"
	//  - It should be exist if DLQ is enabled and topic is not set.
	//
	//  - Available variables:
	//    - AppName
	FormatDLQTopic string `cfg:"format_dlq_topic" json:"format_dlq_topic"`
	// Validation is a configuration for validation when consumer initialized.
	Validation Validation `cfg:"validation" json:"validation"`
}

// configApply configuration to ConsumerConfig and check validation.
func configApply(c ConsumerPreConfig, consumerConfig *ConsumerConfig, progName string, logger Logger) error {
	if c.PrefixGroupID != "" {
		consumerConfig.GroupID = c.PrefixGroupID + consumerConfig.GroupID
	}

	if !consumerConfig.DLQ.Disabled && consumerConfig.DLQ.Topic == "" && c.FormatDLQTopic == "" {
		consumerConfig.DLQ.Disabled = true
		logger.Warn("dlq is disabled because topic and format_dlq_topic is not set")
	}

	// add default topic name for DLQ
	if !consumerConfig.DLQ.Disabled {
		if consumerConfig.DLQ.Topic == "" {
			if c.FormatDLQTopic == "" {
				return fmt.Errorf("format_dlq_topic is required if dlq topic is not set")
			}

			var err error
			consumerConfig.DLQ.Topic, err = templateRun(c.FormatDLQTopic, map[string]string{"AppName": progName})
			if err != nil {
				return fmt.Errorf("format_dlq_topic: %w", err)
			}
		}

		if consumerConfig.Skip == nil {
			consumerConfig.Skip = map[string]map[int32]OffsetConfig{
				consumerConfig.DLQ.Topic: consumerConfig.DLQ.Skip,
			}
		} else {
			if _, ok := consumerConfig.Skip[consumerConfig.DLQ.Topic]; !ok {
				consumerConfig.Skip[consumerConfig.DLQ.Topic] = consumerConfig.DLQ.Skip
			}
		}

		if consumerConfig.DLQ.RetryInterval == 0 {
			consumerConfig.DLQ.RetryInterval = DefaultRetryInterval
		}

		if consumerConfig.DLQ.RetryMaxInterval == 0 {
			consumerConfig.DLQ.RetryMaxInterval = DefaultRetryMaxInterval
		}
	}

	if err := c.Validation.Validate(consumerConfig); err != nil {
		return fmt.Errorf("validate consumer config: %w", err)
	}

	return nil
}

// Validation is a configuration for validation when consumer initialized.
type Validation struct {
	GroupID GroupIDValidation `cfg:"group_id" json:"group_id"`
}

// GroupIDValidation is a configuration for group_id validation.
type GroupIDValidation struct {
	Enabled bool `cfg:"enabled" json:"enabled"`
	// RgxGroupID is a regex pattern to validate RgxGroupID.
	RgxGroupID string `cfg:"rgx_group_id" json:"rgx_group_id"`
}

func (v GroupIDValidation) Validate(groupID string) error {
	if !v.Enabled {
		return nil
	}

	if groupID == "" {
		return fmt.Errorf("group_id is required")
	}

	if v.RgxGroupID != "" {
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

func (v Validation) Validate(consumerConfig *ConsumerConfig) error {
	if err := v.GroupID.Validate(consumerConfig.GroupID); err != nil {
		return err
	}

	return nil
}
