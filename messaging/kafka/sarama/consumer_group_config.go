package sarama

import (
	"fmt"
	"time"

	"github.com/fedotovmax/microservice-core/network"
	"github.com/kelseyhightower/envconfig"
)

type ConsumerGroupConfig struct {
	Brokers []string `envconfig:"KAFKA_CONSUMER_GROUP_BROKERS" required:"true"`
	Topics  []string `envconfig:"KAFKA_CONSUMER_GROUP_TOPICS" required:"true"`
	GroupID string   `envconfig:"KAFKA_CONSUMER_GROUP_GROUP_ID" required:"true"`

	BackoffMaxInterval time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_BACKOFF_MAX_INTERVAL" required:"true" default:"25s"`
	CommitInterval     time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_COMMIT_INTERVAL" required:"true" default:"10s"`
	BackoffMinInterval time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_BACKOFF_MIN_INTERVAL" required:"true" default:"1s"`
	MaxProcessingTime  time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_MAX_PROCESSING_TIME" required:"true" default:"10s"`

	DialTimeout       time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_DIAL_TIMEOUT" default:"5s"`
	ReadTimeout       time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_READ_TIMEOUT" default:"10s"`
	SessionTimeout    time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_SESSION_TIMEOUT" default:"30s"`
	HeartbeatInterval time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_HEARTBEAT_INTERVAL" default:"3s"`
	RebalanceTimeout  time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_REBALANCE_TIMEOUT" default:"60s"`
	MaxWaitTime       time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_MAX_WAIT_TIME" default:"500ms"`

	//CommitBatchSize    int           `envconfig:"KAFKA_CONSUMER_GROUP_COMMIT_BATCH_SIZE" required:"true" default:"30"`
}

func (c ConsumerGroupConfig) Validate() error {

	const op = "core.messaging.kafka.sarama.ConsumerGroupConfig.Validate"

	// 1. Проверка брокеров и топиков
	if len(c.Brokers) == 0 {
		return fmt.Errorf("%s: at least one broker is required", op)
	}
	for i := range c.Brokers {
		if err := network.Addr(c.Brokers[i]); err != nil {
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

	// 2. Базовые поля
	if c.GroupID == "" {
		return fmt.Errorf("%s: group id must be greater than or equal to 1 character", op)
	}

	// 3. Интервалы ретраев и коммитов
	if c.BackoffMaxInterval < time.Second {
		return fmt.Errorf("%s: backoff max interval must be >= 1s", op)
	}
	if c.BackoffMinInterval < time.Second {
		return fmt.Errorf("%s: backoff min interval must be >= 1s", op)
	}
	if c.BackoffMinInterval > c.BackoffMaxInterval {
		return fmt.Errorf("%s: backoff min interval cannot be greater than max interval", op)
	}
	if c.CommitInterval < time.Second {
		return fmt.Errorf("%s: commit interval must be >= 1s", op)
	}

	// 4. Тайм-ауты сессии и Heartbeat (Критично!)
	if c.SessionTimeout < time.Second*6 {
		return fmt.Errorf("%s: session timeout is too low (min 6s recommended)", op)
	}
	if c.HeartbeatInterval < time.Second {
		return fmt.Errorf("%s: heartbeat interval must be >= 1s", op)
	}
	// Правило Kafka: SessionTimeout должен быть минимум в 3 раза больше Heartbeat
	if c.HeartbeatInterval*3 > c.SessionTimeout {
		return fmt.Errorf("%s: heartbeat interval must be at least 3x less than session timeout", op)
	}

	// 5. Обработка и ожидание
	if c.MaxProcessingTime < time.Millisecond*500 {
		return fmt.Errorf("%s: max processing time is too low (min 500ms recommended)", op)
	}
	if c.RebalanceTimeout < time.Second*5 {
		return fmt.Errorf("%s: rebalance timeout must be >= 5s", op)
	}
	if c.MaxWaitTime < time.Millisecond*100 {
		return fmt.Errorf("%s: max wait time must be >= 100ms", op)
	}

	// 6. Сетевые тайм-ауты
	if c.DialTimeout < time.Second {
		return fmt.Errorf("%s: dial timeout must be >= 1s", op)
	}
	if c.ReadTimeout < time.Second {
		return fmt.Errorf("%s: read timeout must be >= 1s", op)
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
