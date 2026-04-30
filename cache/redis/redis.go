package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type client struct {
	rc  *redis.Client
	log *slog.Logger
}

func New(ctx context.Context, config Config, log *slog.Logger) (*client, error) {

	const op = "core.cache.redis.New"

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("%s: error when validate config: %w", op, err)
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr:            config.Addr,
		Password:        config.Password,
		DB:              config.DB,
		MaxRetries:      config.MaxRetries,
		MinRetryBackoff: config.MinRetryBackoff,
		MaxRetryBackoff: config.MaxRetryBackoff,
		PoolSize:        config.PoolSize,
		MaxIdleConns:    config.MaxIdleConns,
		ConnMaxLifetime: config.MaxConnLifetime,
		ConnMaxIdleTime: config.MaxIdleConnLifetime,
	})

	err := redisClient.Ping(ctx).Err()

	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &client{
		rc:  redisClient,
		log: log,
	}, nil

}

func (p *client) Ping(ctx context.Context) error {

	const op = "core.cache.redis.client.Ping"

	err := p.rc.Ping(ctx).Err()

	return fmt.Errorf("%s: ping error: %w", op, err)
}

func (p *client) Set(ctx context.Context, key string, value any, exp time.Duration) error {

	const op = "core.cache.redis.client.Set"

	err := p.rc.Set(ctx, key, value, exp).Err()

	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil

}

func (p *client) SetIfNotExist(ctx context.Context, key string, value any, exp time.Duration) error {

	const op = "core.cache.redis.client.SetIfNotExist"

	err := p.rc.SetArgs(ctx, key, value, redis.SetArgs{
		Mode: "NX",
		TTL:  exp,
	}).Err()

	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("%s: %w", op, ErrKeyExists)
		}
		return err
	}

	return nil
}

func (p *client) Get(ctx context.Context, key string) (string, error) {

	const op = "core.cache.redis.client.Get"

	value, err := p.rc.Get(ctx, key).Result()

	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("%s: %w", op, ErrKeyNotExists)
		}
		return "", err
	}
	return value, nil
}

func (p *client) GetInt64(ctx context.Context, key string) (int64, error) {

	const op = "core.cache.redis.client.GetInt64"

	value, err := p.rc.Get(ctx, key).Int64()

	if err != nil {
		if err == redis.Nil {
			return 0, fmt.Errorf("%s: %w", op, ErrKeyNotExists)
		}
		return 0, err
	}
	return value, nil
}

func (p *client) GetBool(ctx context.Context, key string) (bool, error) {

	const op = "core.cache.redis.client.GetBool"

	value, err := p.rc.Get(ctx, key).Bool()

	if err != nil {
		if err == redis.Nil {
			return false, fmt.Errorf("%s: %w", op, ErrKeyNotExists)
		}
		return false, err
	}
	return value, nil
}

func (p *client) GetFloat64(ctx context.Context, key string) (float64, error) {

	const op = "core.cache.redis.client.GetFloat64"

	value, err := p.rc.Get(ctx, key).Float64()

	if err != nil {
		if err == redis.Nil {
			return 0, fmt.Errorf("%s: %w", op, ErrKeyNotExists)
		}
		return 0, err
	}
	return value, nil
}

func (p *client) GetBytes(ctx context.Context, key string, dest any) ([]byte, error) {

	const op = "core.cache.redis.client.GetBytes"

	value, err := p.rc.Get(ctx, key).Bytes()

	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("%s: %w", op, ErrKeyNotExists)
		}
		return nil, err
	}
	return value, nil
}

func (p *client) GetJSON(ctx context.Context, key string, dest any) error {

	const op = "core.cache.redis.client.GetJSON"

	value, err := p.rc.Get(ctx, key).Bytes()

	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("%s: %w", op, ErrKeyNotExists)
		}
		return err
	}

	err = json.Unmarshal(value, dest)

	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (p *client) Delete(ctx context.Context, keys ...string) error {

	const op = "core.cache.redis.client.Delete"

	if err := p.rc.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil

}

func (p *client) IncInt64(ctx context.Context, key string, exp time.Duration) (int64, error) {

	const op = "core.cache.redis.client.IncInt64"

	i, err := p.IncInt64By(ctx, key, 1, exp)

	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	return i, nil
}

func (p *client) IncInt64By(ctx context.Context, key string, incr int64, exp time.Duration) (int64, error) {

	const op = "core.cache.redis.client.IncInt64By"

	res, err := luaIncrInt.Run(ctx, p.rc, []string{key}, incr, exp.Milliseconds()).Int64()

	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	return res, nil
}

func (p *client) IncFloat64(ctx context.Context, key string, exp time.Duration) (float64, error) {

	const op = "core.cache.redis.client.IncFloat64"

	f, err := p.IncFloat64By(ctx, key, 1.0, exp)

	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}

	return f, nil
}

func (p *client) IncFloat64By(ctx context.Context, key string, incr float64, exp time.Duration) (float64, error) {

	const op = "core.cache.redis.client.IncFloat64By"

	res, err := luaIncrFloat.Run(ctx, p.rc, []string{key}, incr, exp.Milliseconds()).Float64()

	if err != nil {
		return 0, fmt.Errorf("%s: %w", op, err)
	}
	return res, nil
}

func (r *client) Stop(ctx context.Context) error {

	const op = "core.cache.redis.client.Stop"

	done := make(chan error, 1)

	go func() {
		err := r.rc.Close()
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: %w", op, ctx.Err())
	}
}
