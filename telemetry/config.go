package telemetry

import (
	"fmt"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

type ConfigOption func(*Config)

func WithMetricsProviderExportInterval(d time.Duration) ConfigOption {
	return func(c *Config) {
		c.MetricsProviderExportInterval = d
	}
}

func WithServiceVersion(v string) ConfigOption {
	return func(c *Config) {
		c.ServiceVersion = v
	}
}

func WithMetricsViews(views ...sdkmetric.View) ConfigOption {
	return func(c *Config) {
		c.MetricsViews = views
	}
}

func WithMetricsProviderTimeout(d time.Duration) ConfigOption {
	return func(c *Config) {
		c.MetricsProviderTimeout = d
	}
}
func WithMetricsExporterTimeout(d time.Duration) ConfigOption {
	return func(c *Config) {
		c.MetricsExporterTimeout = d
	}
}

func WithMetricsExporterRetryStartInterval(d time.Duration) ConfigOption {
	return func(c *Config) {
		c.MetricsExporterRetryStartInterval = d
	}
}
func WithMetricsExporterRetryMaxInterval(d time.Duration) ConfigOption {
	return func(c *Config) {
		c.MetricsExporterRetryMaxInterval = d
	}
}
func WithMetricsExporterRetryMaxTime(d time.Duration) ConfigOption {
	return func(c *Config) {
		c.MetricsExporterRetryMaxTime = d
	}
}

func WithTracesBatchTimeout(d time.Duration) ConfigOption {
	return func(c *Config) {
		c.TracesBatchTimeout = d
	}
}

func WithTracesExporterTimeout(d time.Duration) ConfigOption {
	return func(c *Config) {
		c.TracesExporterTimeout = d
	}
}

func WithTracesExporterRetryStartInterval(d time.Duration) ConfigOption {
	return func(c *Config) {
		c.TracesExporterRetryStartInterval = d
	}
}
func WithTracesExporterRetryMaxInterval(d time.Duration) ConfigOption {
	return func(c *Config) {
		c.TracesExporterRetryMaxInterval = d
	}
}
func WithTracesExporterRetryMaxTime(d time.Duration) ConfigOption {
	return func(c *Config) {
		c.TracesExporterRetryMaxTime = d
	}
}

func WithTracesMaxQueueSize(v int) ConfigOption {
	return func(c *Config) {
		c.TracesMaxQueueSize = v
	}
}

func WithTracesExportBatchSize(v int) ConfigOption {
	return func(c *Config) {
		c.TracesExportBatchSize = v
	}
}

type Config struct {
	Environment string

	ServiceName    string
	ServiceVersion string
	CollectorAddr  string

	TracesExporterTimeout            time.Duration
	TracesBatchTimeout               time.Duration
	TracesExporterRetryStartInterval time.Duration
	TracesExporterRetryMaxInterval   time.Duration
	TracesExporterRetryMaxTime       time.Duration

	TracesMaxQueueSize    int // kb
	TracesExportBatchSize int // kb

	MetricsProviderExportInterval     time.Duration
	MetricsProviderTimeout            time.Duration
	MetricsExporterTimeout            time.Duration
	MetricsExporterRetryStartInterval time.Duration
	MetricsExporterRetryMaxInterval   time.Duration
	MetricsExporterRetryMaxTime       time.Duration
	MetricsViews                      []sdkmetric.View
}

func (c *Config) Validate() error {

	const op = "core.telemetry.Config.Validate"

	if c.CollectorAddr == "" {
		return fmt.Errorf("%s: collector address cannot be empty string", op)
	}

	if c.ServiceName == "" {
		return fmt.Errorf("%s: service name cannot be empty string", op)
	}

	if c.MetricsProviderExportInterval < time.Second {
		return fmt.Errorf("%s: metrics export interval cannot be less than 1s", op)
	}

	if c.ServiceVersion == "" {
		return fmt.Errorf("%s: service version cannot be empty string", op)
	}

	return nil
}

func defaultConfig() Config {
	return Config{
		ServiceVersion: "v1.0.0",

		MetricsProviderExportInterval:     5 * time.Second,
		MetricsProviderTimeout:            5 * time.Second,
		MetricsExporterTimeout:            5 * time.Second,
		MetricsExporterRetryStartInterval: 1 * time.Second,
		MetricsExporterRetryMaxInterval:   10 * time.Second,
		MetricsExporterRetryMaxTime:       15 * time.Second,

		TracesExporterRetryStartInterval: 1 * time.Second,
		TracesExporterRetryMaxInterval:   5 * time.Second,
		TracesExporterRetryMaxTime:       30 * time.Second,
		TracesExporterTimeout:            5 * time.Second,
		TracesMaxQueueSize:               65536,
		TracesExportBatchSize:            4096,
		TracesBatchTimeout:               2 * time.Second,
	}
}

func NewConfig(env, name, addr string, opts ...ConfigOption) (Config, error) {

	cfg := defaultConfig()
	cfg.Environment = env
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

func NewConfigMust(env, name, addr string, opts ...ConfigOption) Config {

	cfg, err := NewConfig(env, name, addr, opts...)

	if err != nil {
		panic(err)
	}

	return cfg
}
