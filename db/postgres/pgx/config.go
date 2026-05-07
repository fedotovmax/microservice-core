package pgx

import (
	"fmt"
	"net/url"
	"time"
)

type Option func(*BaseConfig)

type BaseConfig struct {
	RetryWaitFrom       time.Duration
	MaxConnLifetime     time.Duration
	MaxIdleConnLifetime time.Duration
	MaxConns            int
	MinConns            int
	MaxRetries          int
	Tracing             bool
}

func WithTracing(f bool) Option {
	return func(c *BaseConfig) {
		c.Tracing = f
	}
}

func WithRetryWaitFrom(d time.Duration) Option {
	return func(c *BaseConfig) {
		c.RetryWaitFrom = d
	}
}

func WithMaxConnLifetime(d time.Duration) Option {
	return func(c *BaseConfig) {
		c.MaxConnLifetime = d
	}
}

func WithMaxIdleConnLifetime(d time.Duration) Option {
	return func(c *BaseConfig) {
		c.MaxIdleConnLifetime = d
	}
}

func WithMaxConns(n int) Option {
	return func(c *BaseConfig) {
		c.MaxConns = n
	}
}

func WithMinConns(n int) Option {
	return func(c *BaseConfig) {
		c.MinConns = n
	}
}

func WithMaxRetries(n int) Option {
	return func(c *BaseConfig) {
		c.MaxRetries = n
	}
}

func defaultBaseConfig() BaseConfig {
	return BaseConfig{
		RetryWaitFrom:       5 * time.Second,
		MaxConnLifetime:     30 * time.Minute,
		MaxIdleConnLifetime: 5 * time.Minute,
		MaxConns:            20,
		MinConns:            5,
		MaxRetries:          5,
		Tracing:             false,
	}
}

func (b *BaseConfig) Validate() error {
	const op = "core.db.postgres.pgx.BaseConfig.Validate"

	// 1. Проверка соединений (Connection Pool)
	// MaxConns не может быть меньше 1, иначе приложение не сможет сделать ни одного запроса.
	if b.MaxConns <= 0 {
		return fmt.Errorf("%s: max conns must be at least 1", op)
	}

	// MinConns может быть 0 (ленивое создание соединений), но не меньше.
	if b.MinConns < 0 {
		return fmt.Errorf("%s: min conns cannot be negative", op)
	}

	// Критическая логическая ошибка: пул не заведется, если минимум больше максимума.
	if b.MinConns > b.MaxConns {
		return fmt.Errorf("%s: min conns (%d) cannot be greater than max conns (%d)", op, b.MinConns, b.MaxConns)
	}

	// 2. Проверка жизненного цикла (Lifetimes)
	// Время жизни соединения должно быть положительным, чтобы пул мог ими управлять.
	if b.MaxConnLifetime <= 0 {
		return fmt.Errorf("%s: max conn lifetime must be a positive duration", op)
	}

	// Время простоя (Idle) логично держать меньше или равным общему времени жизни.
	if b.MaxIdleConnLifetime < 0 {
		return fmt.Errorf("%s: max idle conn lifetime cannot be negative", op)
	}

	if b.MaxIdleConnLifetime > b.MaxConnLifetime {
		return fmt.Errorf("%s: max idle conn lifetime cannot exceed MaxConnLifetime", op)
	}

	// 3. Ретраи

	if b.MaxRetries < 0 {
		return fmt.Errorf("%s: max retries cannot be negative", op)
	}

	// Если мы планируем повторять запросы, пауза между ними должна быть физически ощутимой.
	if b.MaxRetries > 0 && b.RetryWaitFrom <= 0 {
		return fmt.Errorf("%s: retry wait from must be positive when retries are enabled", op)
	}

	return nil
}

type Config struct {
	BaseConfig
	Dsn string
}

func NewConfig(dsn string, opts ...Option) (Config, error) {
	base := defaultBaseConfig()

	for _, opt := range opts {
		opt(&base)
	}

	cfg := Config{
		BaseConfig: base,
		Dsn:        dsn,
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func NewConfigMust(dsn string, opts ...Option) Config {

	config, err := NewConfig(dsn)

	if err != nil {
		panic(err)
	}

	return config
}

func (c *Config) Validate() error {
	const op = "core.db.postgres.pgx.Config.Validate"

	if _, err := url.Parse(c.Dsn); err != nil {
		return fmt.Errorf("%s: invalid postgres connection url: %w", op, err)
	}

	if err := c.BaseConfig.Validate(); err != nil {
		return fmt.Errorf("%s: error when validate base config: %w", op, err)
	}

	return nil

}

type ShardedConfig struct {
	BaseConfig
	Shards []string
}

func NewShardedConfig(shards []string, opts ...Option) (ShardedConfig, error) {
	base := defaultBaseConfig()

	for _, opt := range opts {
		opt(&base)
	}

	cfg := ShardedConfig{
		BaseConfig: base,
		Shards:     shards,
	}

	if err := cfg.Validate(); err != nil {
		return ShardedConfig{}, err
	}

	return cfg, nil
}

func NewShardedConfigMust(shards []string, opts ...Option) ShardedConfig {

	config, err := NewShardedConfig(shards, opts...)

	if err != nil {
		panic(err)
	}

	return config
}

func (c *ShardedConfig) Validate() error {
	const op = "core.db.postgres.pgx.ShardedConfig.Validate"

	if len(c.Shards) == 0 {
		return fmt.Errorf("%s: shards cannot be empty", op)
	}

	for _, dsn := range c.Shards {
		if _, err := url.Parse(dsn); err != nil {
			return fmt.Errorf("%s: invalid postgres connection url: %w", op, err)
		}
	}

	if err := c.BaseConfig.Validate(); err != nil {
		return fmt.Errorf("%s: error when validate base config: %w", op, err)
	}

	return nil
}
