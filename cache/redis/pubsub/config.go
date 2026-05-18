package pubsub

import (
	"time"
)

type Config struct {
	PubSubRetryWaitFrom time.Duration
	MaxRetryBackoff     time.Duration
	MaxRetries          int
}

func (c *Config) Validate() error {

	return nil
}

type Option func(*Config)

func WithPubSubRetryWaitFrom(t time.Duration) Option {
	return func(c *Config) {
		c.PubSubRetryWaitFrom = t
	}
}

func WithMaxRetryBackoff(b time.Duration) Option {
	return func(c *Config) {
		c.MaxRetryBackoff = b
	}
}

func WithMaxRetries(n int) Option {
	return func(c *Config) {
		c.MaxRetries = n
	}
}

func defaultConfig() Config {
	return Config{
		MaxRetryBackoff:     100 * time.Second,
		MaxRetries:          3,
		PubSubRetryWaitFrom: 3 * time.Second,
	}
}

func NewConfig(opts ...Option) (Config, error) {
	cfg := defaultConfig()

	for _, opt := range opts {
		opt(&cfg)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func NewConfigMust(opts ...Option) Config {

	config, err := NewConfig(opts...)

	if err != nil {
		panic(err)
	}

	return config
}
