package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type Redis interface {
	Set(ctx context.Context, key string, value any, exp time.Duration) error
	IncInt64(ctx context.Context, key string, exp time.Duration) (int64, error)
	IncInt64By(ctx context.Context, key string, incr int64, exp time.Duration) (int64, error)
	IncFloat64(ctx context.Context, key string, incr float64, exp time.Duration) (float64, error)
	IncFloat64By(ctx context.Context, key string, incr float64, exp time.Duration) (float64, error)
	SetIfNotExist(ctx context.Context, key string, value any, exp time.Duration) error
	Delete(ctx context.Context, keys ...string) error
	Get(ctx context.Context, key string) (string, error)
	GetInt64(ctx context.Context, key string) (int64, error)
	GetFloat64(ctx context.Context, key string) (float64, error)
	GetBool(ctx context.Context, key string) (bool, error)
	GetBytes(ctx context.Context, key string, dest any) ([]byte, error)
	GetJSON(ctx context.Context, key string, dest any) error
	Stop(ctx context.Context) error
}

type pool struct {
	*redis.Client
	log *slog.Logger
}

func New(ctx context.Context, config Config, log *slog.Logger) (*pool, error) {

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

	_, err := redisClient.Ping(ctx).Result()

	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &pool{
		Client: redisClient,
		log:    log,
	}, nil

}

func (p *pool) Set(ctx context.Context, key string, value any, exp time.Duration) error {

	err := p.Client.Set(ctx, key, value, exp).Err()

	if err != nil {
		return err
	}

	return nil

}

func (p *pool) SetIfNotExist(ctx context.Context, key string, value any, exp time.Duration) error {

	err := p.Client.SetArgs(ctx, key, value, redis.SetArgs{
		Mode: "NX",
		TTL:  exp,
	}).Err()

	if err != nil {
		if err == redis.Nil {
			return ErrKeyExists
		}
		return err
	}

	return nil
}

func (p *pool) Get(ctx context.Context, key string) (string, error) {

	value, err := p.Client.Get(ctx, key).Result()

	if err != nil {
		if err == redis.Nil {
			return "", ErrKeyNotExists
		}
		return "", err
	}
	return value, nil
}

func (p *pool) GetInt64(ctx context.Context, key string) (int64, error) {

	value, err := p.Client.Get(ctx, key).Int64()

	if err != nil {
		if err == redis.Nil {
			return 0, ErrKeyNotExists
		}
		return 0, err
	}
	return value, nil
}

func (p *pool) GetBool(ctx context.Context, key string) (bool, error) {

	value, err := p.Client.Get(ctx, key).Bool()

	if err != nil {
		if err == redis.Nil {
			return false, ErrKeyNotExists
		}
		return false, err
	}
	return value, nil
}

func (p *pool) GetFloat64(ctx context.Context, key string) (float64, error) {

	value, err := p.Client.Get(ctx, key).Float64()

	if err != nil {
		if err == redis.Nil {
			return 0, ErrKeyNotExists
		}
		return 0, err
	}
	return value, nil
}

func (p *pool) GetBytes(ctx context.Context, key string, dest any) ([]byte, error) {

	value, err := p.Client.Get(ctx, key).Bytes()

	if err != nil {
		if err == redis.Nil {
			return nil, ErrKeyNotExists
		}
		return nil, err
	}
	return value, nil
}

func (p *pool) GetJSON(ctx context.Context, key string, dest any) error {

	value, err := p.Client.Get(ctx, key).Bytes()

	if err != nil {
		if err == redis.Nil {
			return ErrKeyNotExists
		}
		return err
	}

	err = json.Unmarshal(value, dest)

	if err != nil {
		return err
	}

	return nil
}

func (p *pool) Delete(ctx context.Context, keys ...string) error {

	if err := p.Client.Del(ctx, keys...).Err(); err != nil {
		return err
	}

	return nil

}

// Здесь мы просто пробрасываем exp как Duration дальше
func (p *pool) IncInt64(ctx context.Context, key string, exp time.Duration) (int64, error) {
	return p.IncInt64By(ctx, key, 1, exp)
}

// А здесь уже происходит магия конвертации
func (p *pool) IncInt64By(ctx context.Context, key string, incr int64, exp time.Duration) (int64, error) {

	// Обязательно вызываем .Milliseconds() здесь, чтобы в Lua ушло число, а не объект или строка типа "1s"
	res, err := luaIncrInt.Run(ctx, p.Client, []string{key}, incr, exp.Milliseconds()).Int64()

	if err != nil {
		return 0, err
	}
	return res, nil
}

func (p *pool) IncFloat64(ctx context.Context, key string, exp time.Duration) (float64, error) {
	return p.IncFloat64By(ctx, key, 1.0, exp)
}

// IncFloat64By увеличивает значение ключа на произвольное дробное число
func (p *pool) IncFloat64By(ctx context.Context, key string, incr float64, exp time.Duration) (float64, error) {

	// Используем .Float64() для получения результата из скрипта
	// Передаем exp.Milliseconds() в качестве второго аргумента (ARGV[2])
	res, err := luaIncrFloat.Run(ctx, p.Client, []string{key}, incr, exp.Milliseconds()).Float64()

	if err != nil {
		return 0, err
	}
	return res, nil
}

func (r *pool) Stop(ctx context.Context) error {

	const op = "core.cache.redis.Pool.Stop"

	done := make(chan error, 1)

	go func() {
		err := r.Client.Close()
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
