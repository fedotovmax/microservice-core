package sarama

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type ConsumerGroupConfig struct {
	Brokers            []string      `envconfig:"BROKERS" required:"true"`
	Topics             []string      `envconfig:"TOPICS" required:"true"`
	BackoffMaxInterval time.Duration `envconfig:"BACKOFF_MAX_INTERVAL" required:"true" default:"25s"`
	CommitInterval     time.Duration `envconfig:"COMMIT_INTERVAL" required:"true" default:"10s"`
	BackoffMinInterval time.Duration `envconfig:"BACKOFF_MIN_INTERVAL" required:"true" default:"1s"`
	MaxProcessingTime  time.Duration `envconfig:"MAX_PROCESSING_TIME" required:"true" default:"300ms"`
	GroupID            string        `envconfig:"GROUP_ID" required:"true"`
	CommitBatchSize    int           `envconfig:"COMMIT_BATCH_SIZE" required:"true" default:"30"`
}

func NewConsumerGroupConfig() (ConsumerGroupConfig, error) {
	var config ConsumerGroupConfig

	if err := envconfig.Process("KAFKA_CONSUMER_GROUP", &config); err != nil {
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

type ProducerConfig struct {
	Brokers            []string      `envconfig:"BROKERS" required:"true"`
	ChannelBufferSize  int           `envconfig:"CHANNEL_BUFFER_SIZE" required:"true"`  // 100
	SendMaxRetries     int           `envconfig:"SEND_MAX_RETRIES" required:"true"`     // 5s
	BatchBytes         int           `envconfig:"BATCH_BYTES" required:"true"`          // 512 * 1024
	BatchMessagesCount int           `envconfig:"BATCH_MESSAGES_COUNT" required:"true"` // 200
	RetryBackoff       time.Duration `envconfig:"RETRY_BACKOFF" required:"true"`        // 100ms
	BatchFrequency     time.Duration `envconfig:"BATCH_FREQUENCY" required:"true"`      // 200ms
}

func NewProducerConfig() (ProducerConfig, error) {
	var config ProducerConfig

	if err := envconfig.Process("KAFKA_PRODUCER", &config); err != nil {
		return ProducerConfig{}, fmt.Errorf("error when parse kafka producer env variables: %w", err)
	}

	return config, nil
}

func NewProducerConfigMust() ProducerConfig {
	config, err := NewProducerConfig()

	if err != nil {
		panic(err)
	}

	return config
}
