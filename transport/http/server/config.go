package server

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Port            int           `envconfig:"PORT" default:"8080"`
	Host            string        `envconfig:"HOST" default:"localhost"`
	ShutdownTimeout time.Duration `envconfig:"SHUTDOWN_TIMEOUT" default:"15s"`
}

func NewConfig() (Config, error) {
	var config Config

	if err := envconfig.Process("HTTP", &config); err != nil {
		return Config{}, fmt.Errorf("error when parse http server env variables: %w", err)
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
