package middlewares

import (
	"context"
	"errors"

	"github.com/fedotovmax/microservice-core/ft"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

// ConsumerRetryMiddleware оборачивает бизнес-логику в механизм повторных попыток.
func ConsumerRetryMiddleware(backoff ft.Backoff, maxRetries int) kafka.Middleware {
	return func(next kafka.MessageHandler) kafka.MessageHandler {
		return func(ctx context.Context, msg kafka.ConsumeMessage) error {

			// Настраиваем политику: ретраим всё, кроме NoRetryError
			policy := func(err error) bool {
				if err == nil {
					return false
				}
				// Если ошибка обернута в NoRetryError - смысла повторять нет
				var noRetryErr *kafka.NoRetryError
				if errors.As(err, &noRetryErr) {
					return false
				}
				// В остальных случаях (сеть, таймауты БД) - пробуем еще раз
				return true
			}

			// Вызываем ft.Retry
			return ft.Retry(ctx, backoff, maxRetries, policy, func() error {
				return next(ctx, msg)
			})
		}
	}
}
