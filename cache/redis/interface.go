package redis

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// Client — платформенный контракт для Redis.
// Даёт доступ к нативному клиенту + управление lifecycle.
type Client interface {
	// Native возвращает нативный *redis.Client для прямой работы с Redis.
	Native() UniversalClient

	// Ping проверяет соединение.
	Ping(ctx context.Context) error

	// Stop gracefully закрывает соединение с Redis.
	// Вызывать только после того как остановлен pubsub.PubSub.
	Stop(ctx context.Context) error
}

type UniversalClient interface {
	redis.Cmdable
	PubSubClient
}

type PubSubClient interface {
	Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
}
