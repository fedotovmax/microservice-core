package zap

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

type Level string

const (
	LevelDebug   Level = "debug"
	LevelInfo    Level = "info"
	LevelWarning Level = "warning"
	LevelError   Level = "error"
	LevelPanic   Level = "panic"
	LevelFatal   Level = "fatal"
)

func (l Level) String() string {
	return string(l)
}

type Env string

const (
	EnvProduction  Env = "production"
	EnvDevelopment Env = "development"
)

type Config struct {
	Level         Level  `envconfig:"LEVEL" default:"debug"`
	LogFolderPath string `envconfig:"LOG_FOLDER_PATH" required:"true"`
	Env           Env    `envconfig:"ENV" default:"development"`
}

func NewConfig() (Config, error) {

	var config Config

	if err := envconfig.Process("LOGGER", &config); err != nil {
		return Config{}, fmt.Errorf("error when parse logger env variables: %w", err)
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
