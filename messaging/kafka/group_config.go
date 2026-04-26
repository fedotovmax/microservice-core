package kafka

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

// - ReadTimeout → сколько ждём ответ (важно для commit)
// - WriteTimeout → сколько отправляем запрос
// - Session.Timeout → когда тебя выкинут из группы
// - Heartbeat.Interval → как часто ты “пингуешь” Kafka
type GroupConfig struct {
	Brokers []string `envconfig:"KAFKA_CONSUMER_GROUP_BROKERS" required:"true"`
	Topics  []string `envconfig:"KAFKA_CONSUMER_GROUP_TOPICS" required:"true"`
	GroupID string   `envconfig:"KAFKA_CONSUMER_GROUP_GROUP_ID" required:"true"`

	BackoffMaxInterval time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_BACKOFF_MAX_INTERVAL" required:"true" default:"25s"`
	BackoffMinInterval time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_BACKOFF_MIN_INTERVAL" required:"true" default:"1s"`
	CommitInterval     time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_COMMIT_INTERVAL" required:"true" default:"10s"`
	MaxProcessingTime  time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_MAX_PROCESSING_TIME" required:"true" default:"10s"`

	DialTimeout    time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_DIAL_TIMEOUT" default:"5s"`
	ReadTimeout    time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_READ_TIMEOUT" default:"10s"`
	SessionTimeout time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_SESSION_TIMEOUT" default:"30s"`

	// HeartbeatInterval only for sarama, do not provide if use segmentio
	HeartbeatInterval time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_HEARTBEAT_INTERVAL" default:"3s"`
	RebalanceTimeout  time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_REBALANCE_TIMEOUT" default:"60s"`

	// MaxWaitTime only for sarama, do not provide if use segmentio
	MaxWaitTime time.Duration `envconfig:"KAFKA_CONSUMER_GROUP_MAX_WAIT_TIME" default:"500ms"`
}

func (c GroupConfig) Validate() error {

	const op = "core.messaging.kafka.GroupConfig.Validate"

	if len(c.Brokers) == 0 {
		return fmt.Errorf("%s: at least one broker is required", op)
	}

	for i := range c.Brokers {
		if c.Brokers[i] == "" {
			return fmt.Errorf("%s: broker address with index: %d is empty", op, i)
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

func NewGroupConfig() (GroupConfig, error) {
	var config GroupConfig

	if err := envconfig.Process("", &config); err != nil {
		return GroupConfig{}, fmt.Errorf("error when parse kafka consumer group env variables: %w", err)
	}

	return config, nil
}
func NewGroupConfigMust() GroupConfig {
	config, err := NewGroupConfig()

	if err != nil {
		panic(err)
	}

	return config
}
