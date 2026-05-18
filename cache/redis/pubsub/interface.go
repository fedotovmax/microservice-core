package pubsub

import (
	"context"
)

// PubSub — платформенный контракт для pub/sub поверх Redis.
// Имеет собственный lifecycle независимый от redis.Client.
type PubSub interface {
	// Publish публикует сообщение в канал.
	Publish(ctx context.Context, channel string, value any) error

	// Subscribe подписывается на каналы и запускает handler в воркерпуле.
	// Автоматически переподключается при обрыве соединения.
	// Подписка живёт пока не вызван Stop или не отменён exCtx.
	Subscribe(exCtx context.Context, handler Handler, channels ...string)

	// Stop gracefully останавливает все подписки и ждёт завершения воркеров.
	Stop(ctx context.Context) error
}
