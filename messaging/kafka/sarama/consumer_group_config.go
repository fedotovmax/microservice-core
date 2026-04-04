package sarama

import (
	"fmt"
	"time"

	"github.com/fedotovmax/microservice-core/network"
	"github.com/kelseyhightower/envconfig"
)

type ConsumerGroupConfig struct {
	Brokers            []string      `envconfig:"KAFKA_CONSUMER_GROUP_BROKERS" required:"true"`
	Topics             []string      `envconfig:"KAFKA_CONSUMER_GROUP_TOPICS" required:"true"`
	GroupID            string        `envconfig:"KAFKA_CONSUMER_GROUP_GROUP_ID" required:"true"`
	BackoffMaxInterval time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_BACKOFF_MAX_INTERVAL" required:"true" default:"25s"`
	CommitInterval     time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_COMMIT_INTERVAL" required:"true" default:"10s"`
	BackoffMinInterval time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_BACKOFF_MIN_INTERVAL" required:"true" default:"1s"`
	MaxProcessingTime  time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_MAX_PROCESSING_TIME" required:"true" default:"300ms"`
	CommitBatchSize    int           `envconfig:"KAFKA_CONSUMER_GROUP_COMMIT_BATCH_SIZE" required:"true" default:"30"`
}

func (c ConsumerGroupConfig) Validate() error {

	const op = "messaging.kafka.sarama.ConsumerGroupConfig.Validate"

	if len(c.Brokers) == 0 {
		return fmt.Errorf("%s: at least one broker is required", op)
	}

	for i := range c.Brokers {
		err := network.Addr(c.Brokers[i])
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
	}

	if len(c.Topics) == 0 {
		return fmt.Errorf("%s: at least one topic is required", op)
	}

	for i := range c.Topics {
		if len(c.Topics[i]) == 0 {
			return fmt.Errorf("%s: topic name must be at least 1 character", op)
		}
	}

	if c.BackoffMaxInterval < time.Second {
		return fmt.Errorf("%s: backoff max interval must be greater than or equal to 1 second", op)
	}

	if c.CommitInterval < time.Second {
		return fmt.Errorf("%s: commit interval must be greater than or equal to 1 second", op)
	}

	if c.BackoffMinInterval < time.Second {
		return fmt.Errorf("%s: backoff min interval must be greater than or equal to 1 second", op)
	}

	if c.MaxProcessingTime < time.Millisecond*50 {
		return fmt.Errorf("%s: max processing time must be greater than or equal to 50 milliseconds", op)
	}

	if c.GroupID == "" {
		return fmt.Errorf("%s: group id must be greater than or equal to 1 character", op)
	}

	if c.CommitBatchSize < 1 {
		return fmt.Errorf("%s: commit batch size must be greater than or equal to 1", op)
	}

	return nil
}

func NewConsumerGroupConfig() (ConsumerGroupConfig, error) {
	var config ConsumerGroupConfig

	if err := envconfig.Process("", &config); err != nil {
		return ConsumerGroupConfig{}, fmt.Errorf("error when parse kafka consumer group env variables: %w", err)
	}

	return config, nil
}
func NewConsumerGroupConfigMust() ConsumerGroupConfig {
	config, err := NewConsumerGroupConfig()

	if err != nil {
		panic(err)
	}

	return config
}
