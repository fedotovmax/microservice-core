package outbox

import (
	"fmt"
	"time"
)

type Option func(*Config)

type Config struct {
	// BatchLimit — количество событий, вычитываемых из БД за одну итерацию.
	// ВАЖНО: Это значение должно быть МЕНЬШЕ или РАВНО значению
	// KAFKA_PRODUCER_CHANNEL_BUFFER_SIZE.
	// Если батч из БД будет больше буфера продюсера, отправка станет блокирующей,
	// что замедлит обработку и может привести к истечению ReserveDuration.
	BatchLimit int

	// Interval — пауза между итерациями опроса БД.
	Interval time.Duration

	// ReserveDuration — время "заморозки" строки в БД для одного инстанса.
	// Должно быть > (BatchLimit * (SendTimeout + HandleSuccessTimeout)).
	ReserveDuration time.Duration

	// SendTimeout — таймаут на передачу сообщения в канал Sarama.
	SendTimeout time.Duration

	// HandleSuccessTimeout — таймаут на отметку успешной отправки в БД.
	HandleSuccessTimeout time.Duration

	// HandleErrorTimeout — таймаут на отметку ошибки отправки в БД.
	HandleErrorTimeout time.Duration
}

const (
	minLimit    = 10
	maxLimit    = 1000
	minInterval = 350 * time.Millisecond
	// Поднимаем минимальный порог операции до 500мс.
	// 300мс — это слишком "тонко" для сетевых запросов к БД под нагрузкой.
	minOperationTimeout = 500 * time.Millisecond
)

func WithBatchLimit(n int) Option {
	return func(c *Config) {
		c.BatchLimit = n
	}
}

func WithInterval(d time.Duration) Option {
	return func(c *Config) {
		c.Interval = d
	}
}

func WithReserveDuration(d time.Duration) Option {
	return func(c *Config) {
		c.ReserveDuration = d
	}
}

func WithSendTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.SendTimeout = d
	}
}

func WithHandleSuccessTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.HandleSuccessTimeout = d
	}
}

func WithHandleErrorTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.HandleErrorTimeout = d
	}
}

const (
	defaultBatchLimit           = 100
	defaultInterval             = 2 * time.Second
	defaultReserveDuration      = 5 * time.Minute
	defaultSendTimeout          = 2 * time.Second
	defaultHandleSuccessTimeout = 1 * time.Second
	defaultHandleErrorTimeout   = 1 * time.Second
)

func defaultConfig() *Config {
	return &Config{
		BatchLimit:           defaultBatchLimit,
		Interval:             defaultInterval,
		ReserveDuration:      defaultReserveDuration,
		SendTimeout:          defaultSendTimeout,
		HandleSuccessTimeout: defaultHandleSuccessTimeout,
		HandleErrorTimeout:   defaultHandleErrorTimeout,
	}
}

func (c *Config) Validate() error {
	const op = "core.messaging.kafka.outbox.Config.Validate"

	if c.BatchLimit < minLimit || c.BatchLimit > maxLimit {
		return fmt.Errorf("%s: batch limit must be between %d and %d", op, minLimit, maxLimit)
	}

	if c.Interval < minInterval {
		return fmt.Errorf("%s: interval must be at least %v", op, minInterval)
	}

	if c.SendTimeout < minOperationTimeout {
		return fmt.Errorf("%s: send timeout must be at least %v", op, minOperationTimeout)
	}

	if c.HandleSuccessTimeout < minOperationTimeout {
		return fmt.Errorf("%s: handle success timeout must be at least %v", op, minOperationTimeout)
	}

	if c.HandleErrorTimeout < minOperationTimeout {
		return fmt.Errorf("%s: handle error timeout must be at least %v", op, minOperationTimeout)
	}

	maxProcessingTime := time.Duration(c.BatchLimit) * (c.SendTimeout + c.HandleSuccessTimeout)
	if c.ReserveDuration < maxProcessingTime {
		return fmt.Errorf(
			"%s: reserve duration (%v) is too short for batch size %d with given timeouts, minimum safe reserve: %v",
			op, c.ReserveDuration, c.BatchLimit, maxProcessingTime,
		)
	}

	return nil
}

func NewConfig(opts ...Option) (*Config, error) {
	cfg := defaultConfig()

	for _, opt := range opts {
		opt(cfg)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func NewConfigMust(opts ...Option) *Config {
	cfg, err := NewConfig(opts...)
	if err != nil {
		panic(err)
	}
	return cfg
}
