package redis

import (
	"fmt"
	"time"
)

type Config struct {
	MaxRetryBackoff     time.Duration
	MinRetryBackoff     time.Duration
	MaxConnLifetime     time.Duration
	MaxIdleConnLifetime time.Duration
	Addr                string
	Password            string
	DB                  int
	MaxRetries          int
	PoolSize            int
	MaxIdleConns        int
}

func (c Config) Validate() error {
	const op = "core.cache.redis.Config.Validate"

	if c.Addr == "" {
		return fmt.Errorf("%s: connection address cannot be empty", op)
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
