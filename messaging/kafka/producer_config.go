package kafka

import (
	"fmt"
	"time"
)

// Producer

type ProducerOption func(*ProducerConfig)

func WithChannelBufferSize(n int) ProducerOption {
	return func(c *ProducerConfig) {
		c.ChannelBufferSize = n
	}
}

func WithSendMaxRetries(n int) ProducerOption {
	return func(c *ProducerConfig) {
		c.SendMaxRetries = n
	}
}

func WithRetryBackoff(d time.Duration) ProducerOption {
	return func(c *ProducerConfig) {
		c.RetryBackoff = d
	}
}

func WithBatchFrequency(d time.Duration) ProducerOption {
	return func(c *ProducerConfig) {
		c.BatchFrequency = d
	}
}

func WithBatchBytes(n int) ProducerOption {
	return func(c *ProducerConfig) {
		c.BatchBytes = n
	}
}

func WithBatchMessagesCount(n int) ProducerOption {
	return func(c *ProducerConfig) {
		c.BatchMessagesCount = n
	}
}

func WithProducerTelemetry() ProducerOption {
	return func(c *ProducerConfig) {
		c.Telemetry = true
	}
}

func defaultProducerConfig() ProducerConfig {
	return ProducerConfig{
		ChannelBufferSize:  256,
		SendMaxRetries:     3,
		RetryBackoff:       time.Millisecond * 300,
		BatchFrequency:     100 * time.Millisecond,
		BatchBytes:         1048576, // 1MB
		BatchMessagesCount: 100,
		Telemetry:          false,
	}
}

func NewProducerConfig(brokers []string, opts ...ProducerOption) (ProducerConfig, error) {
	cfg := defaultProducerConfig()
	cfg.Brokers = brokers

	for _, opt := range opts {
		opt(&cfg)
	}

	if err := cfg.Validate(); err != nil {
		return ProducerConfig{}, err
	}

	return cfg, nil
}

func NewProducerConfigMust(brokers []string, opts ...ProducerOption) ProducerConfig {
	cfg, err := NewProducerConfig(brokers, opts...)
	if err != nil {
		panic(err)
	}
	return cfg
}

type ProducerConfig struct {
	Brokers []string
	// Увеличиваем буфер канала. Если он будет меньше BatchLimit из Outbox,
	// метод Send() будет блокироваться и тормозить цикл вычитки из БД.
	ChannelBufferSize int

	// Ретраи на уровне Sarama.
	// Для Outbox лучше ставить 3-5, чтобы быстрее получить ошибку и
	// вернуть событие в обработку через БД, а не висеть в памяти продюсера.
	SendMaxRetries int
	RetryBackoff   time.Duration
	// Батчинг (Flush). Эти настройки определяют, когда Sarama реально отправит пакет в сеть.
	BatchFrequency time.Duration
	BatchBytes     int // 1MB - хороший стандарт
	// Должен коррелировать с BatchLimit в Outbox.
	// Если в Outbox лимит 100, а тут 300, Sarama будет ждать 3 итерации Outbox или таймаута Frequency.
	BatchMessagesCount int

	Telemetry bool
}

func (c *ProducerConfig) Validate() error {
	const op = "core.messaging.kafka.ProducerConfig.Validate"

	if len(c.Brokers) == 0 {
		return fmt.Errorf("%s: at least one broker is required", op)
	}

	for i := range c.Brokers {
		if c.Brokers[i] == "" {
			return fmt.Errorf("%s: broker address with index: %d is empty", op, i)
		}
	}

	// Минимально необходимый ретрай — 1.
	// Если 0, Sarama при первой же сетевой заминке выкинет ошибку в HandleErrors.
	if c.SendMaxRetries < 1 {
		return fmt.Errorf("%s: send max retries must be at least 1", op)
	}

	if c.RetryBackoff < time.Millisecond*10 {
		return fmt.Errorf("%s: retry backoff is too aggressive, min 10ms", op)
	}

	// Если ChannelBufferSize будет 0, канал будет небуферизированным.
	// В AsyncProducer это приведет к тому, что каждое сообщение будет ждать
	// подтверждения приема горутиной Sarama, что уничтожит производительность.
	if c.ChannelBufferSize < 100 {
		return fmt.Errorf("%s: channel buffer size must be at least 100 for stable async work", op)
	}

	if c.BatchMessagesCount <= 0 {
		return fmt.Errorf("%s: batch messages count must be greater than 0", op)
	}

	return nil
}
