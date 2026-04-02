package zap

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

type Level string

func (l Level) String() string { return string(l) }

func (l Level) Validate() error {

	const op = "core.logger.zap.Level.Validate"

	switch l {
	case LevelDebug, LevelInfo, LevelWarning, LevelError, LevelPanic, LevelFatal:
		return nil
	default:
		return fmt.Errorf("%s: %w", op, ErrInvalidLogLevel())
	}
}

const (
	LevelDebug   Level = "debug"
	LevelInfo    Level = "info"
	LevelWarning Level = "warning"
	LevelError   Level = "error"
	LevelPanic   Level = "panic"
	LevelFatal   Level = "fatal"
)

type Env string

func (e Env) String() string { return string(e) }

func (e Env) Validate() error {

	const op = "core.logger.zap.Env.Validate"

	switch e {
	case EnvDevelopment, EnvProduction:
		return nil
	default:
		return fmt.Errorf("%s: %w", op, ErrInvalidEnv())
	}
}

const (
	EnvProduction  Env = "production"
	EnvDevelopment Env = "development"
)

type LogFolder struct {
	Enable bool   `envconfig:"ENABLE" default:"false"`
	Path   string `envconfig:"PATH"`
}

func (f LogFolder) Validate() error {

	const op = "core.logger.zap.LogFolder.Validate"

	if f.Enable && f.Path == "" {
		return fmt.Errorf("%s: log folder path is required when log folder is enabled", op)
	}
	return nil
}

type Config struct {
	Level     Level     `envconfig:"LEVEL" default:"debug"`
	LogFolder LogFolder `envconfig:"LOG_FOLDER"`
	Env       Env       `envconfig:"ENV" default:"development"`
}

func (c Config) Validate() error {

	const op = "core.logger.zap.Config.Validate"

	err := c.LogFolder.Validate()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err = c.Level.Validate()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	err = c.Env.Validate()
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil

}

func NewConfig() (Config, error) {

	const op = "core.logger.zap.NewConfig"

	var config Config

	err := envconfig.Process("LOGGER", &config)
	if err != nil {
		return Config{}, fmt.Errorf("%s: error when parse logger env variables: %w", op, err)
	}

	err = config.Validate()
	if err != nil {
		return Config{}, fmt.Errorf("%s: error when validate logger config: %w", op, err)
	}

	return config, nil
}

func NewConfigMust() Config {

	config, err := NewConfig()

	if err != nil {
		panic(err)
	}

	return config
}
