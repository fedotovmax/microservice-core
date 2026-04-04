package redis

import (
	"fmt"
	"time"

	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	MaxRetryBackoff     time.Duration `envconfig:"REDIS_MAX_RETRY_BACKOFF" default:"100s"`
	MinRetryBackoff     time.Duration `envconfig:"REDIS_MIN_RETRY_BACKOFF" default:"1s"`
	MaxConnLifetime     time.Duration `envconfig:"REDIS_MAX_CONN_LIFETIME" default:"60m"`
	MaxIdleConnLifetime time.Duration `envconfig:"REDIS_MAX_IDLE_CONN_LIFETIME" default:"10m"`
	Addr                string        `envconfig:"REDIS_ADDR" required:"true"`
	Password            string        `envconfig:"REDIS_PASSWORD" required:"true"`
	DB                  int           `envconfig:"REDIS_DB" default:"0"`
	MaxRetries          uint8         `envconfig:"REDIS_MAX_RETRIES" default:"5"`
	PoolSize            int           `envconfig:"REDIS_POOL_SIZE" default:"20"`
	MaxIdleConns        int           `envconfig:"REDIS_MAX_IDLE_CONNECTIONS" default:"5"`
}

func NewConfig() (Config, error) {
	var config Config

	if err := envconfig.Process("", &config); err != nil {
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
