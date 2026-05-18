package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func Get(ctx context.Context, rc redis.Cmdable, key string) Cmd {
	return newCmd(rc.Get(ctx, key))
}

// Set записывает значение. exp=0 — без TTL.
func Set(ctx context.Context, rc redis.Cmdable, key string, value any, exp time.Duration) error {
	if err := rc.Set(ctx, key, value, exp).Err(); err != nil {
		return fmt.Errorf("redis.Set %q: %w", key, err)
	}
	return nil
}

// SetJSON сериализует value в JSON и записывает. exp=0 — без TTL.
func SetJSON(ctx context.Context, rc redis.Cmdable, key string, value any, exp time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("redis.SetJSON %q: marshal: %w", key, err)
	}
	if err := rc.Set(ctx, key, data, exp).Err(); err != nil {
		return fmt.Errorf("redis.SetJSON %q: %w", key, err)
	}
	return nil
}

// SetNX записывает значение только если ключ не существует.
// Возвращает ErrKeyExists если ключ уже есть.
func SetNX(ctx context.Context, rc redis.Cmdable, key string, value any, exp time.Duration) error {
	err := rc.SetArgs(ctx, key, value, redis.SetArgs{
		Mode: "NX",
		TTL:  exp,
	}).Err()
	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("redis.SetNX %q: %w", key, ErrKeyExists)
		}
		return fmt.Errorf("redis.SetNX %q: %w", key, err)
	}
	return nil
}

// Delete удаляет ключи.
func Delete(ctx context.Context, rc redis.Cmdable, keys ...string) error {
	if err := rc.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("redis.Delete: %w", err)
	}
	return nil
}

// Incr атомарно инкрементирует int64 и устанавливает TTL если ключ новый.
func Incr(ctx context.Context, rc redis.Cmdable, key string, exp time.Duration) (int64, error) {
	return IncrBy(ctx, rc, key, 1, exp)
}

// IncrBy атомарно инкрементирует int64 на incr и устанавливает TTL если ключ новый.
func IncrBy(ctx context.Context, rc redis.Cmdable, key string, incr int64, exp time.Duration) (int64, error) {
	res, err := luaIncrInt.Run(ctx, rc, []string{key}, incr, exp.Milliseconds()).Int64()
	if err != nil {
		return 0, fmt.Errorf("redis.IncrBy %q: %w", key, err)
	}
	return res, nil
}

// IncrFloat атомарно инкрементирует float64 и устанавливает TTL если ключ новый.
func IncrFloat(ctx context.Context, rc redis.Cmdable, key string, exp time.Duration) (float64, error) {
	return IncrFloatBy(ctx, rc, key, 1.0, exp)
}

// IncrFloatBy атомарно инкрементирует float64 на incr и устанавливает TTL если ключ новый.
func IncrFloatBy(ctx context.Context, rc redis.Cmdable, key string, incr float64, exp time.Duration) (float64, error) {
	res, err := luaIncrFloat.Run(ctx, rc, []string{key}, incr, exp.Milliseconds()).Float64()
	if err != nil {
		return 0, fmt.Errorf("redis.IncrFloatBy %q: %w", key, err)
	}
	return res, nil
}

// HSet записывает hash. exp=0 — без TTL.
func HSet(ctx context.Context, rc redis.Cmdable, key string, values map[string]any, exp time.Duration) error {
	pipe := rc.Pipeline()
	pipe.HSet(ctx, key, values)
	if exp > 0 {
		pipe.Expire(ctx, key, exp)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis.HSet %q: %w", key, err)
	}
	return nil
}

// HGet возвращает поле из hash.
func HGet(ctx context.Context, rc redis.Cmdable, key, field string) (string, error) {
	val, err := rc.HGet(ctx, key, field).Result()
	if err != nil {
		if err == redis.Nil {
			return "", fmt.Errorf("redis.HGet %q: %w", key, ErrKeyNotExists)
		}
		return "", fmt.Errorf("redis.HGet %q: %w", key, err)
	}
	return val, nil
}

// HGetAll возвращает все поля hash.
func HGetAll(ctx context.Context, rc redis.Cmdable, key string) (map[string]string, error) {
	values, err := rc.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("redis.HGetAll %q: %w", key, err)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("redis.HGetAll %q: %w", key, ErrKeyNotExists)
	}
	return values, nil
}
