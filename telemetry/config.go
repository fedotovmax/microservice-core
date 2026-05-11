package telemetry

import (
	"fmt"
	"time"
)

type ConfigOption func(*Config)

func WithMetricsExportInterval(d time.Duration) ConfigOption {
	return func(c *Config) {
		c.MetricsExportInterval = d
	}
}

func WithServiceVersion(v string) ConfigOption {
	return func(c *Config) {
		c.ServiceVersion = v
	}
}

type Config struct {
	ServiceName           string
	ServiceVersion        string
	CollectorAddr         string
	MetricsExportInterval time.Duration
}

func (c *Config) Validate() error {

	const op = "core.telemetry.Config.Validate"

	if c.CollectorAddr == "" {
		return fmt.Errorf("%s: collector address cannot be empty string", op)
	}

	if c.ServiceName == "" {
		return fmt.Errorf("%s: service name cannot be empty string", op)
	}

	if c.MetricsExportInterval < time.Second {
		return fmt.Errorf("%s: metrics export interval cannot be less than 1s", op)
	}

	if c.ServiceVersion == "" {
		return fmt.Errorf("%s: service version cannot be empty string", op)
	}

	return nil
}

func defaultConfig() Config {
	return Config{
		MetricsExportInterval: time.Second * 5,
		ServiceVersion:        "v1.0.0",
	}
}

func NewConfig(name, addr string, opts ...ConfigOption) (Config, error) {

	cfg := defaultConfig()
	cfg.CollectorAddr = addr
	cfg.ServiceName = name

	for _, opt := range opts {
		opt(&cfg)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func NewConfigMust(name, addr string, opts ...ConfigOption) Config {

	cfg, err := NewConfig(name, addr, opts...)

	if err != nil {
		panic(err)
	}

	return cfg
}
