package server

import (
	"fmt"
	"time"
)

type Option func(*Config)

func WithReadTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.ReadTimeout = d
	}
}

func WithReadHeaderTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.ReadHeaderTimeout = d
	}
}

func WithWriteTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.WriteTimeout = d
	}
}

func WithIdleTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.IdleTimeout = d
	}
}

func WithOnStartErrorHandlerTimeout(d time.Duration) Option {
	return func(c *Config) {
		c.OnStartErrorHandlerTimeout = d
	}
}

func defaultConfig() *Config {
	return &Config{
		ReadTimeout:                5 * time.Second,
		ReadHeaderTimeout:          3 * time.Second,
		WriteTimeout:               10 * time.Second,
		IdleTimeout:                120 * time.Second,
		OnStartErrorHandlerTimeout: 5 * time.Second,
	}
}

func NewConfig(addr string, opts ...Option) (*Config, error) {
	cfg := defaultConfig()
	cfg.Addr = addr

	for _, opt := range opts {
		opt(cfg)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func NewConfigMust(addr string, opts ...Option) *Config {

	config, err := NewConfig(addr, opts...)

	if err != nil {
		panic(err)
	}

	return config
}

type Config struct {
	Addr                       string
	ReadTimeout                time.Duration
	ReadHeaderTimeout          time.Duration
	WriteTimeout               time.Duration
	IdleTimeout                time.Duration
	OnStartErrorHandlerTimeout time.Duration
}

func (c Config) Validate() error {

	const op = "transport.http.server.Config.Validate"

	if c.Addr == "" {
		return fmt.Errorf("%s: grpc listen addr cannot be empty", op)
	}

	return nil
}
