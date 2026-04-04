package jwt

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Secret string `envconfig:"JWT_SECRET" required:"true"`
	Issuer string `envconfig:"JWT_ISSUER" required:"true"`
}

func NewConfig() (Config, error) {

	const op = "core.security.jwt.NewConfig"

	var config Config

	err := envconfig.Process("", &config)

	if err != nil {
		return Config{}, fmt.Errorf("%s: error when parse jwt env variables: %w", op, err)
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
