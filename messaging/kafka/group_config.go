package kafka

import (
	"fmt"
	"time"
)

// Consumer Group

type GroupOption func(*GroupConfig)

func WithBackoffMaxInterval(d time.Duration) GroupOption {
	return func(c *GroupConfig) {
		c.BackoffMaxInterval = d
	}
}

func WithBackoffMinInterval(d time.Duration) GroupOption {
	return func(c *GroupConfig) {
		c.BackoffMinInterval = d
	}
}

func WithCommitInterval(d time.Duration) GroupOption {
	return func(c *GroupConfig) {
		c.CommitInterval = d
	}
}

func WithMaxProcessingTime(d time.Duration) GroupOption {
	return func(c *GroupConfig) {
		c.MaxProcessingTime = d
	}
}

func WithDialTimeout(d time.Duration) GroupOption {
	return func(c *GroupConfig) {
		c.DialTimeout = d
	}
}

func WithReadTimeout(d time.Duration) GroupOption {
	return func(c *GroupConfig) {
		c.ReadTimeout = d
	}
}

func WithSessionTimeout(d time.Duration) GroupOption {
	return func(c *GroupConfig) {
		c.SessionTimeout = d
	}
}

func WithHeartbeatInterval(d time.Duration) GroupOption {
	return func(c *GroupConfig) {
		c.HeartbeatInterval = d
	}
}

func WithRebalanceTimeout(d time.Duration) GroupOption {
	return func(c *GroupConfig) {
		c.RebalanceTimeout = d
	}
}

func WithMaxWaitTime(d time.Duration) GroupOption {
	return func(c *GroupConfig) {
		c.MaxWaitTime = d
	}
}

func defaultGroupConfig() *GroupConfig {
	return &GroupConfig{
		BackoffMaxInterval: 25 * time.Second,
		BackoffMinInterval: 1 * time.Second,
		CommitInterval:     10 * time.Second,
		MaxProcessingTime:  10 * time.Second,
		DialTimeout:        5 * time.Second,
		ReadTimeout:        10 * time.Second,
		SessionTimeout:     30 * time.Second,
		HeartbeatInterval:  3 * time.Second,
		RebalanceTimeout:   60 * time.Second,
		MaxWaitTime:        500 * time.Millisecond,
	}
}

func NewGroupConfig(brokers []string, topics []string, groupID string, opts ...GroupOption) (*GroupConfig, error) {
	cfg := defaultGroupConfig()
	cfg.Brokers = brokers
	cfg.Topics = topics
	cfg.GroupID = groupID

	for _, opt := range opts {
		opt(cfg)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func NewGroupConfigMust(brokers []string, topics []string, groupID string, opts ...GroupOption) *GroupConfig {
	cfg, err := NewGroupConfig(brokers, topics, groupID, opts...)
	if err != nil {
		panic(err)
	}
	return cfg
}

// - ReadTimeout → сколько ждём ответ (важно для commit)
// - WriteTimeout → сколько отправляем запрос
// - Session.Timeout → когда тебя выкинут из группы
// - Heartbeat.Interval → как часто ты “пингуешь” Kafka
type GroupConfig struct {
	Brokers []string
	Topics  []string
	GroupID string

	BackoffMaxInterval time.Duration
	BackoffMinInterval time.Duration
	CommitInterval     time.Duration
	MaxProcessingTime  time.Duration

	DialTimeout    time.Duration
	ReadTimeout    time.Duration
	SessionTimeout time.Duration

	// HeartbeatInterval only for sarama, do not provide if use segmentio
	HeartbeatInterval time.Duration
	RebalanceTimeout  time.Duration

	// MaxWaitTime only for sarama, do not provide if use segmentio
	MaxWaitTime time.Duration
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
