package server

import (
	"fmt"
	"time"
)

type Option func(*Config)

func WithOnStartErrorHandlersTimeout(t time.Duration) Option {
	return func(c *Config) {
		c.OnStartErrorHandlersTimeout = t
	}
}

type Config struct {
	Addr string
	//TODO: use pattern optional config
	OnStartErrorHandlersTimeout time.Duration
}

func defaultConfig() Config {
	return Config{
		OnStartErrorHandlersTimeout: time.Second * 5,
	}
}

func NewConfig(addr string, opts ...Option) (Config, error) {
	cfg := defaultConfig()
	cfg.Addr = addr

	for _, opt := range opts {
		opt(&cfg)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func NewConfigMust(addr string, opts ...Option) Config {

	config, err := NewConfig(addr, opts...)

	if err != nil {
		panic(err)
	}

	return config
}

func (c Config) Validate() error {

	const op = "transport.grpc.server.Config.Validate"

	if c.Addr == "" {
		return fmt.Errorf("%s: grpc listen addr cannot be empty", op)
	}

	if c.OnStartErrorHandlersTimeout == 0 {
		return fmt.Errorf("%s: OnStartErrorHandlersTimeout cannot be 0", op)

	}

	return nil
}
