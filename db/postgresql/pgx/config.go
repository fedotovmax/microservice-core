package pgx

import (
	"fmt"
	"net/url"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	BaseConfig
	Dsn string `envconfig:"POSTGRES_DSN" required:"true"`
}

func (c Config) Validate() error {
	const op = "core.db.postgresql.pgx.Config.Validate"

	if _, err := url.Parse(c.Dsn); err != nil {
		return fmt.Errorf("%s: invalid postgres connection url: %w", op, err)
	}

	if err := c.BaseConfig.Validate(); err != nil {
		return fmt.Errorf("%s: error when validate base config: %w", op, err)
	}

	return nil

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
