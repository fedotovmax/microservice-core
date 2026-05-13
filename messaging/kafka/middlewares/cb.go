package middlewares

import (
	"context"

	"github.com/fedotovmax/microservice-core/ft"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

// ConsumerCircuitBreakerMiddleware защищает внешние системы от перегрузки,
// если они начали массово отвечать ошибками.
func ConsumerCircuitBreakerMiddleware(cb ft.CircuitBreaker) kafka.Middleware {
	return func(next kafka.MessageHandler) kafka.MessageHandler {
		return func(ctx context.Context, msg kafka.ConsumeMessage) error {

			// Оборачиваем вызов хендлера в Circuit Breaker
			return cb.Execute(func() error {
				return next(ctx, msg)
			})

		}
	}
}
