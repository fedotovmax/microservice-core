package redis

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	MinRetryBackoff     time.Duration `envconfig:"MIN_RETRY_BACKOFF" default:"1s"`
	MaxRetryBackoff     time.Duration `envconfig:"MAX_RETRY_BACKOFF" default:"100s"`
	MaxConnLifetime     time.Duration `envconfig:"MAX_CONN_LIFETIME" default:"60m"`
	MaxIdleConnLifetime time.Duration `envconfig:"MAX_IDLE_CONN_LIFETIME" default:"10m"`
	Addr                string        `envconfig:"ADDR" required:"true"`
	Host                string        `envconfig:"HOST" required:"true"`
	Port                int           `envconfig:"PORT" required:"true"`
	Password            string        `envconfig:"PASSWORD" required:"true"`
	DB                  int           `envconfig:"DB" default:"0"`
	MaxRetries          uint8         `envconfig:"MAX_RETRIES" default:"5"`
	PoolSize            int           `envconfig:"POOL_SIZE" default:"20"`
	MaxIdleConns        int           `envconfig:"MAX_IDLE_CONNECTIONS" default:"5"`
}

func NewConfig() (Config, error) {
	var config Config

	if err := envconfig.Process("REDIS", &config); err != nil {
		return Config{}, fmt.Errorf("error when parse redis env variables: %w", err)
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
