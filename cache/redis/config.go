package redis

import (
	"fmt"
	"time"
)

type ConfigOption func(*Config)

type Config struct {
	MaxRetryBackoff     time.Duration
	MinRetryBackoff     time.Duration
	MaxConnLifetime     time.Duration
	MaxIdleConnLifetime time.Duration
	Addr                string
	Password            string
	DB                  int
	MaxRetries          int
	PoolSize            int
	MaxIdleConns        int
}

func WithMaxRetryBackoff(b time.Duration) ConfigOption {
	return func(c *Config) {
		c.MaxRetryBackoff = b
	}
}

func WithMinRetryBackoff(b time.Duration) ConfigOption {
	return func(c *Config) {
		c.MinRetryBackoff = b
	}
}

func WithMaxConnLifetime(d time.Duration) ConfigOption {
	return func(c *Config) {
		c.MaxConnLifetime = d
	}
}

func WithMaxIdleConnLifetime(d time.Duration) ConfigOption {
	return func(c *Config) {
		c.MaxIdleConnLifetime = d
	}
}

func WithMaxRetries(n int) ConfigOption {
	return func(c *Config) {
		c.MaxRetries = n
	}
}

func WithPoolSize(n int) ConfigOption {
	return func(c *Config) {
		c.PoolSize = n
	}
}

func WithMaxIdleConns(n int) ConfigOption {
	return func(c *Config) {
		c.MaxIdleConns = n
	}
}

func defaultConfig() *Config {
	return &Config{
		MaxRetryBackoff:     100 * time.Second,
		MinRetryBackoff:     1 * time.Second,
		MaxConnLifetime:     60 * time.Minute,
		MaxIdleConnLifetime: 10 * time.Minute,
		MaxRetries:          5,
		PoolSize:            20,
		MaxIdleConns:        5,
	}
}

func NewConfig(addr, password string, db int, opts ...ConfigOption) (*Config, error) {
	cfg := defaultConfig()
	cfg.Addr = addr
	cfg.Password = password
	cfg.DB = db

	for _, opt := range opts {
		opt(cfg)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func NewConfigMust(addr, password string, db int, opts ...ConfigOption) *Config {

	config, err := NewConfig(addr, password, db, opts...)

	if err != nil {
		panic(err)
	}

	return config
}

func (c Config) Validate() error {
	const op = "core.cache.redis.Config.Validate"

	if c.Addr == "" {
		return fmt.Errorf("%s: connection address cannot be empty", op)
	}

	if c.MinRetryBackoff > c.MaxRetryBackoff {
		return fmt.Errorf("%s: min retry backoff (%v) cannot be greater than max (%v)",
			op, c.MinRetryBackoff, c.MaxRetryBackoff)
	}

	if c.MaxIdleConns > c.PoolSize {
		return fmt.Errorf("%s: max idle connections (%d) cannot be greater than pool size (%d)",
			op, c.MaxIdleConns, c.PoolSize)
	}

	if c.MaxIdleConnLifetime > c.MaxConnLifetime && c.MaxConnLifetime != 0 {
		return fmt.Errorf("%s: max idle conn lifetime (%v) cannot exceed max conn lifetime (%v)",
			op, c.MaxIdleConnLifetime, c.MaxConnLifetime)
	}

	if c.DB < 0 {
		return fmt.Errorf("%s: invalid redis db index: %d", op, c.DB)
	}

	return nil

}
