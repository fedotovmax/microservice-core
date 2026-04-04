package pgx

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

// TODO: validate
type Config struct {
	Base
	Dsn string `envconfig:"POSTGRES_DSN" required:"true"`
}

func NewConfig() (Config, error) {

	const op = "core.db.postgresql.pgx.NewConfig"

	var config Config

	if err := envconfig.Process("", &config); err != nil {
		return Config{}, fmt.Errorf("%s: error when parse postgres env variables: %w", op, err)
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
