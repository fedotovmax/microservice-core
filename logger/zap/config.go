package zap

import (
	"fmt"
)

type Option func(*Config)

func WithLevel(l Level) Option {
	return func(c *Config) {
		c.Level = l
	}
}

func WithEncoding(e Encoding) Option {
	return func(c *Config) {
		c.Encoding = e
	}
}

func WithLogFolder(path string) Option {
	return func(c *Config) {
		c.LogFolder = LogFolder{
			Enabled: true,
			Path:    path,
		}
	}
}

func defaultConfig() Config {
	return Config{
		Level:    LevelDebug,
		Encoding: EncodingPlainText,
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

func NewConfigMust() Config {

	config, err := NewConfig()

	if err != nil {
		panic(err)
	}

	return config
}

type Level string

func (l Level) String() string { return string(l) }

func (l Level) Validate() error {

	const op = "core.logger.zap.Level.Validate"

	switch l {
	case LevelDebug, LevelInfo, LevelWarning, LevelError, LevelPanic, LevelFatal:
		return nil
	default:
		return fmt.Errorf("%s: %w", op, InvalidLogLevelError(l))
	}
}

func NewLevel(l string) (Level, error) {

	level := Level(l)

	if err := level.Validate(); err != nil {
		return "", err
	}

	return level, nil
}

const (
	LevelDebug   Level = "debug"
	LevelInfo    Level = "info"
	LevelWarning Level = "warning"
	LevelError   Level = "error"
	LevelPanic   Level = "panic"
	LevelFatal   Level = "fatal"
)

type Encoding string

func (e Encoding) String() string { return string(e) }

func (e Encoding) Validate() error {

	const op = "core.logger.zap.Encoding.Validate"

	switch e {
	case EncodingJSON, EncodingPlainText:
		return nil
	default:
		return fmt.Errorf("%s: %w", op, InvalidEncodingError(e))
	}
}

const (
	EncodingPlainText Encoding = "plain-text"
	EncodingJSON      Encoding = "json"
)

type LogFolder struct {
	Enabled bool
	Path    string
}

func (f LogFolder) Validate() error {

	const op = "core.logger.zap.LogFolder.Validate"

	if f.Enabled && f.Path == "" {
		return fmt.Errorf("%s: log folder path is required when log folder is enabled", op)
	}
	return nil
}

type Config struct {
	LogFolder LogFolder
	Level     Level
	Encoding  Encoding
}

func (c *Config) Validate() error {

	const op = "core.logger.zap.Config.Validate"

	err := c.LogFolder.Validate()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err = c.Level.Validate()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err = c.Encoding.Validate()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil

}
