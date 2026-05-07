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
	PubSubRetryWaitFrom time.Duration
	Addr                string
	Password            string
	DB                  int
	MaxRetries          int
	PoolSize            int
	MaxIdleConns        int
	Tracing             bool
}

func WithTracing(f bool) ConfigOption {
	return func(c *Config) {
		c.Tracing = f
	}
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

func WithPubSubRetryWaitFrom(t time.Duration) ConfigOption {
	return func(c *Config) {
		c.PubSubRetryWaitFrom = t
	}
}

func defaultConfig() Config {
	return Config{
		MaxRetryBackoff:     100 * time.Second,
		MinRetryBackoff:     1 * time.Second,
		MaxConnLifetime:     60 * time.Minute,
		MaxIdleConnLifetime: 10 * time.Minute,
		PubSubRetryWaitFrom: 3 * time.Second,
		MaxRetries:          5,
		PoolSize:            20,
		MaxIdleConns:        5,
		Tracing:             false,
	}
}

func NewConfig(addr, password string, db int, opts ...ConfigOption) (Config, error) {
	cfg := defaultConfig()
	cfg.Addr = addr
	cfg.Password = password
	cfg.DB = db

	for _, opt := range opts {
		opt(&cfg)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func NewConfigMust(addr, password string, db int, opts ...ConfigOption) Config {

	config, err := NewConfig(addr, password, db, opts...)

	if err != nil {
		panic(err)
	}

	return config
}

func (c *Config) Validate() error {

	if c.Addr == "" {
		return fmt.Errorf("redis connection address cannot be empty")
	}

	if c.MinRetryBackoff > c.MaxRetryBackoff {
		return fmt.Errorf("redis min retry backoff (%v) cannot be greater than max (%v)",
			c.MinRetryBackoff, c.MaxRetryBackoff)
	}

	if c.MaxIdleConns > c.PoolSize {
		return fmt.Errorf("redis max idle connections (%d) cannot be greater than pool size (%d)",
			c.MaxIdleConns, c.PoolSize)
	}

	if c.MaxIdleConnLifetime > c.MaxConnLifetime && c.MaxConnLifetime != 0 {
		return fmt.Errorf("redis max idle conn lifetime (%v) cannot exceed max conn lifetime (%v)",
			c.MaxIdleConnLifetime, c.MaxConnLifetime)
	}

	if c.DB < 0 {
		return fmt.Errorf("redis db index must be greater than or equal 0: %d", c.DB)
	}

	return nil

}
