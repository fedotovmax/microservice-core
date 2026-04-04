package sarama

import (
	"fmt"
	"time"

	"github.com/fedotovmax/microservice-core/network"
	"github.com/kelseyhightower/envconfig"
)

type ProducerConfig struct {
	Brokers           []string `envconfig:"KAFKA_PRODUCER_BROKERS" required:"true"`
	ChannelBufferSize int      `envconfig:"KAFKA_PRODUCER_CHANNEL_BUFFER_SIZE" required:"true"`
	//Ретраи
	SendMaxRetries int           `envconfig:"KAFKA_PRODUCER_SEND_MAX_RETRIES" required:"true"`
	RetryBackoff   time.Duration `envconfig:"KAFKA_PRODUCER_RETRY_BACKOFF" required:"true"`
	//Батчинг
	BatchFrequency     time.Duration `envconfig:"KAFKA_PRODUCER_BATCH_FREQUENCY" default:"250ms"`
	BatchBytes         int           `envconfig:"KAFKA_PRODUCER_BATCH_BYTES" default:"262144"`
	BatchMessagesCount int           `envconfig:"KAFKA_PRODUCER_BATCH_MESSAGES_COUNT" default:"300"`
}

func (c ProducerConfig) Validate() error {

	const op = "messaging.kafka.sarama.ProducerConfig.Validate"

	if len(c.Brokers) == 0 {
		return fmt.Errorf("%s: at least one broker is required", op)
	}

	for i := range c.Brokers {
		err := network.Addr(c.Brokers[i])
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
	}

	if c.SendMaxRetries <= 0 {
		return fmt.Errorf("%s: send max retries must be greater than or equal 1", op)
	}

	if c.RetryBackoff < time.Millisecond*50 {
		return fmt.Errorf("%s: retry backoff must be greater than or equal to 50 milliseconds", op)
	}

	if c.BatchFrequency < time.Millisecond*50 {
		return fmt.Errorf("%s: batch frequency must be greater than or equal to 50 milliseconds", op)
	}

	if c.BatchBytes < 0 {
		return fmt.Errorf("%s: batch bytes must be greater than or equal 0", op)
	}

	if c.BatchMessagesCount < 0 {
		return fmt.Errorf("%s: batch messages count must be greater than or equal 0", op)
	}

	if c.ChannelBufferSize < 0 {
		return fmt.Errorf("%s: channel buffer size must be greater than or equal 0", op)
	}

	return nil
}

func NewProducerConfig() (ProducerConfig, error) {
	var config ProducerConfig

	if err := envconfig.Process("", &config); err != nil {
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
