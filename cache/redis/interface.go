package redis

import (
	"context"
	"time"
)

type Client interface {
	Ping(ctx context.Context) error

	Set(ctx context.Context, key string, value any, exp time.Duration) error
	SetNX(ctx context.Context, key string, value any, exp time.Duration) error

	String(ctx context.Context, key string) (string, error)
	Int64(ctx context.Context, key string) (int64, error)
	Bool(ctx context.Context, key string) (bool, error)
	Float64(ctx context.Context, key string) (float64, error)
	Bytes(ctx context.Context, key string, dest any) ([]byte, error)
	JSON(ctx context.Context, key string, dest any) error

	Delete(ctx context.Context, keys ...string) error

	IncInt64(ctx context.Context, key string, exp time.Duration) (int64, error)
	IncInt64By(ctx context.Context, key string, incr int64, exp time.Duration) (int64, error)
	IncFloat64(ctx context.Context, key string, exp time.Duration) (float64, error)
	IncFloat64By(ctx context.Context, key string, incr float64, exp time.Duration) (float64, error)

	HSet(ctx context.Context, key string, values map[string]any, exp time.Duration) error
	HGet(ctx context.Context, key string, field string) (string, error)
	HGetAll(ctx context.Context, key string) (map[string]string, error)

	Publish(ctx context.Context, channel string, value any) error
	Subscribe(exCtx context.Context, handler Handler, channels ...string)

	Stop(ctx context.Context) error
}
