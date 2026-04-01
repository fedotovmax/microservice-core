package pgx

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	Timeout             time.Duration `envconfig:"TIMEOUT" required:"true"`
	RetryWaitFrom       time.Duration `envconfig:"RETRY_WAIT_FROM" default:"5s"`
	MaxConnLifetime     time.Duration `envconfig:"MAX_CONN_LIFETIME" default:"30m"`
	MaxIdleConnLifetime time.Duration `envconfig:"MAX_IDLE_CONN_LIFETIME" default:"5m"`
	Host                string        `envconfig:"HOST" required:"true"`
	User                string        `envconfig:"USER" required:"true"`
	Password            string        `envconfig:"PASSWORD" required:"true"`
	Database            string        `envconfig:"DATABASE" required:"true"`
	Port                int           `envconfig:"PORT" required:"true"`
	MaxRetries          int           `envconfig:"MAX_RETRIES" default:"5"`
	MaxConns            int           `envconfig:"MAX_CONNECTIONS" default:"20"`
	MinConns            int           `envconfig:"MIN_CONNECTIONS" default:"5"`
}

func NewConfig() (Config, error) {
	var config Config

	if err := envconfig.Process("POSTGRES", &config); err != nil {
		return Config{}, fmt.Errorf("error when parse postgres env variables: %w", err)
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
