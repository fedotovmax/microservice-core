package redis

import (
	"fmt"
	"time"

	"github.com/fedotovmax/microservice-core/network"
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
	MaxRetries          int           `envconfig:"REDIS_MAX_RETRIES" default:"5"`
	PoolSize            int           `envconfig:"REDIS_POOL_SIZE" default:"20"`
	MaxIdleConns        int           `envconfig:"REDIS_MAX_IDLE_CONNECTIONS" default:"5"`
}

func (c Config) Validate() error {
	const op = "core.cache.redis.Config.Validate"

	if err := network.Addr(c.Addr); err != nil {
		return fmt.Errorf("%s: invalid redis address: %w", op, err)
	}

	if c.MinRetryBackoff > c.MaxRetryBackoff {
		return fmt.Errorf("%s: min retry backoff (%v) cannot be greater than max (%v)",
			op, c.MinRetryBackoff, c.MaxRetryBackoff)
	}

	if c.MaxIdleConns > c.PoolSize {
		return fmt.Errorf("%s: max idle connections (%d) cannot be greater than pool size (%d)",
			op, c.MaxIdleConns, c.PoolSize)
	}

	if c.MaxIdleConnLifetime > c.MaxConnLifetime && c.MaxConnLifetime != 0 {
		return fmt.Errorf("%s: max idle conn lifetime (%v) cannot exceed max conn lifetime (%v)",
			op, c.MaxIdleConnLifetime, c.MaxConnLifetime)
	}

	if c.DB < 0 {
		return fmt.Errorf("%s: invalid redis db index: %d", op, c.DB)
	}

	return nil

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
