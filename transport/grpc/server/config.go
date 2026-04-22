package server

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Addr string `envconfig:"GRPC_ADDR" default:":8080"`
}

func (c Config) Validate() error {

	const op = "transport.grpc.server.Config.Validate"

	if c.Addr == "" {
		return fmt.Errorf("%s: grpc listen addr cannot be empty", op)
	}

	return nil
}

func NewConfig() (Config, error) {
	var config Config

	if err := envconfig.Process("", &config); err != nil {
		return Config{}, fmt.Errorf("error when parse grpc server env variables: %w", err)
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
