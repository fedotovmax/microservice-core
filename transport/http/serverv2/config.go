package serverv2

import (
	"fmt"
	"time"

	"github.com/fedotovmax/microservice-core/network"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Port            int           `envconfig:"HTTP_PORT" default:"8080"`
	ShutdownTimeout time.Duration `envconfig:"HTTP_SHUTDOWN_TIMEOUT" default:"15s"`
}

func (c Config) Validate() error {

	const op = "transport.http.server.Config.Validate"

	if err := network.Port(c.Port); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if c.ShutdownTimeout < time.Second {
		return fmt.Errorf("%s:shutdown timeout must be at least 1s", op)
	}

	return nil
}

func NewConfig() (Config, error) {
	var config Config

	if err := envconfig.Process("", &config); err != nil {
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
