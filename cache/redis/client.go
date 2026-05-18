package redis

import (
	"context"
	"fmt"
	"sync"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

type Option func(*client)

func WithLogger(log logger.Logger) Option {
	return func(c *client) {
		c.log = log
	}
}

var (
	once     sync.Once
	instance Client
	initErr  error
)

type client struct {
	rc  *redis.Client
	cfg Config
	log logger.Logger
}

func New(ctx context.Context, cfg Config, opts ...Option) (Client, error) {

	once.Do(func() {
		const op = "core.cache.redis.New"

		if err := cfg.Validate(); err != nil {
			initErr = fmt.Errorf("%s: invalid config: %w", op, err)
			return
		}

		rc := redis.NewClient(&redis.Options{
			Addr:            cfg.Addr,
			Password:        cfg.Password,
			DB:              cfg.DB,
			MaxRetries:      cfg.MaxRetries,
			MinRetryBackoff: cfg.MinRetryBackoff,
			MaxRetryBackoff: cfg.MaxRetryBackoff,
			PoolSize:        cfg.PoolSize,
			MaxIdleConns:    cfg.MaxIdleConns,
			ConnMaxLifetime: cfg.MaxConnLifetime,
			ConnMaxIdleTime: cfg.MaxIdleConnLifetime,
		})

		if err := rc.Ping(ctx).Err(); err != nil {
			initErr = fmt.Errorf("%s: %w", op, err)
			return
		}

		if cfg.Tracing {
			if err := redisotel.InstrumentTracing(rc); err != nil {
				initErr = fmt.Errorf("%s: instrument tracing: %w", op, err)
				return
			}
			if err := redisotel.InstrumentMetrics(rc); err != nil {
				initErr = fmt.Errorf("%s: instrument metrics: %w", op, err)
				return
			}
		}

		c := &client{
			rc:  rc,
			cfg: cfg,
		}

		for _, opt := range opts {
			opt(c)
		}

		instance = c
	})

	if initErr != nil {
		return nil, initErr
	}

	return instance, nil
}

func (c *client) Native() UniversalClient {
	return c.rc
}

func (c *client) Ping(ctx context.Context) error {

	const op = "core.cache.redis.Client.Ping"

	if err := c.rc.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

func (c *client) Stop(ctx context.Context) error {

	const op = "core.cache.redis.Client.Stop"

	done := make(chan error, 1)

	go func() {
		done <- c.rc.Close()
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
		if c.log != nil {
			c.log.With(logger.String("op", op)).Info("Redis connection closed gracefully")
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: %w", op, ctx.Err())
	}
}
