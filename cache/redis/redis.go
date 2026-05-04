package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/redis/go-redis/v9"
)

type ClientOption func(*client)

func WithLogger(log logger.Logger) ClientOption {
	return func(c *client) {
		c.log = log
	}
}

type client struct {
	rc         *redis.Client
	cfg        Config
	log        logger.Logger
	subsWg     sync.WaitGroup
	subsCtx    context.Context
	cancelSubs context.CancelFunc
}

func New(ctx context.Context, config Config, opts ...ClientOption) (Client, error) {

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("%s: error when validate config: %w", opNew, err)
	}

	rc := redis.NewClient(&redis.Options{
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

	err := rc.Ping(ctx).Err()

	if err != nil {
		return nil, fmt.Errorf("%s: %w", opNew, err)
	}

	subsCtx, cancelSubs := context.WithCancel(context.Background())

	redisClient := &client{
		rc:         rc,
		cfg:        config,
		subsCtx:    subsCtx,
		cancelSubs: cancelSubs,
	}

	for _, opt := range opts {
		opt(redisClient)
	}

	return redisClient, nil

}

func (p *client) Ping(ctx context.Context) error {

	err := p.rc.Ping(ctx).Err()

	return fmt.Errorf("%s: ping error: %w", opPing, err)
}

func (p *client) Set(ctx context.Context, key string, value any, exp time.Duration) error {

	err := p.rc.Set(ctx, key, value, exp).Err()

	if err != nil {
		return fmt.Errorf("%s: %w", opSet, err)
	}

	return nil

}

func (p *client) SetNX(ctx context.Context, key string, value any, exp time.Duration) error {

	err := p.rc.SetArgs(ctx, key, value, redis.SetArgs{
		Mode: "NX",
		TTL:  exp,
	}).Err()

	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("%s: %w", opSetNX, ErrKeyExists)
		}
		return err
	}

	return nil
}

func (p *client) String(ctx context.Context, key string) (string, error) {

	value, err := p.rc.Get(ctx, key).Result()

	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("%s: %w", opString, ErrKeyNotExists)
		}
		return "", err
	}
	return value, nil
}

func (p *client) Int64(ctx context.Context, key string) (int64, error) {

	value, err := p.rc.Get(ctx, key).Int64()

	if err != nil {
		if err == redis.Nil {
			return 0, fmt.Errorf("%s: %w", opInt64, ErrKeyNotExists)
		}
		return 0, err
	}
	return value, nil
}

func (p *client) Bool(ctx context.Context, key string) (bool, error) {
	value, err := p.rc.Get(ctx, key).Bool()

	if err != nil {
		if err == redis.Nil {
			return false, fmt.Errorf("%s: %w", opBool, ErrKeyNotExists)
		}
		return false, err
	}
	return value, nil
}

func (p *client) Float64(ctx context.Context, key string) (float64, error) {

	value, err := p.rc.Get(ctx, key).Float64()

	if err != nil {
		if err == redis.Nil {
			return 0, fmt.Errorf("%s: %w", opFloat64, ErrKeyNotExists)
		}
		return 0, err
	}
	return value, nil
}

func (p *client) Bytes(ctx context.Context, key string, dest any) ([]byte, error) {

	value, err := p.rc.Get(ctx, key).Bytes()

	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("%s: %w", opBytes, ErrKeyNotExists)
		}
		return nil, err
	}
	return value, nil
}

func (p *client) JSON(ctx context.Context, key string, dest any) error {

	value, err := p.rc.Get(ctx, key).Bytes()

	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("%s: %w", opJSON, ErrKeyNotExists)
		}
		return err
	}

	err = json.Unmarshal(value, dest)

	if err != nil {
		return fmt.Errorf("%s: %w", opJSON, err)
	}

	return nil
}

func (p *client) Delete(ctx context.Context, keys ...string) error {

	if err := p.rc.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("%s: %w", opDelete, err)
	}

	return nil

}

func (p *client) IncInt64(ctx context.Context, key string, exp time.Duration) (int64, error) {

	i, err := p.IncInt64By(ctx, key, 1, exp)

	if err != nil {
		return 0, fmt.Errorf("%s: %w", opIncInt64, err)
	}

	return i, nil
}

func (p *client) IncInt64By(ctx context.Context, key string, incr int64, exp time.Duration) (int64, error) {

	res, err := luaIncrInt.Run(ctx, p.rc, []string{key}, incr, exp.Milliseconds()).Int64()

	if err != nil {
		return 0, fmt.Errorf("%s: %w", opIncInt64By, err)
	}
	return res, nil
}

func (p *client) IncFloat64(ctx context.Context, key string, exp time.Duration) (float64, error) {

	f, err := p.IncFloat64By(ctx, key, 1.0, exp)

	if err != nil {
		return 0, fmt.Errorf("%s: %w", opIncFloat64, err)
	}

	return f, nil
}

func (p *client) IncFloat64By(ctx context.Context, key string, incr float64, exp time.Duration) (float64, error) {

	res, err := luaIncrFloat.Run(ctx, p.rc, []string{key}, incr, exp.Milliseconds()).Float64()

	if err != nil {
		return 0, fmt.Errorf("%s: %w", opIncFloat64By, err)
	}
	return res, nil
}

func (p *client) HSet(ctx context.Context, key string, values map[string]any, exp time.Duration) error {
	pipe := p.rc.Pipeline()

	pipe.HSet(ctx, key, values)

	if exp > 0 {
		pipe.Expire(ctx, key, exp)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("%s: %w", opHSet, err)
	}

	return nil
}

func (p *client) HGet(ctx context.Context, key string, field string) (string, error) {
	value, err := p.rc.HGet(ctx, key, field).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("%s: %w", opHGet, ErrKeyNotExists)
		}
		return "", fmt.Errorf("%s: %w", opHGet, err)
	}
	return value, nil
}

func (p *client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	values, err := p.rc.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("%s: %w", opHGetAll, err)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("%s: %w", opHGetAll, ErrKeyNotExists)
	}
	return values, nil
}

func (r *client) Stop(ctx context.Context) error {

	r.cancelSubs()

	waitCh := make(chan struct{})

	go func() {
		r.subsWg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		if r.log != nil {
			r.log.With(logger.String("op", opStop)).Info("All redis subscriptions stopped gracefully")
		}
	case <-ctx.Done():
		return fmt.Errorf("%s: timeout waiting for subscriptions to stop: %w", opStop, ctx.Err())
	}

	done := make(chan error, 1)

	go func() {
		err := r.rc.Close()
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("%s: %w", opStop, err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: %w", opStop, ctx.Err())
	}
}
