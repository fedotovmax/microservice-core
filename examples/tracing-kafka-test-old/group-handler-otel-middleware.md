```go
package middlewares

import (
	"context"
	"fmt"

	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Адаптер для чтения заголовков (OpenTelemetry нужен интерфейс TextMapCarrier)
type consumerHeaderCarrier struct {
	headers []kafka.Header
}

func (c consumerHeaderCarrier) Get(key string) string {
	for _, h := range c.headers {
		// OTel ищет ключи в нижнем регистре
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}
func (c consumerHeaderCarrier) Set(k, v string) {} // Set не нужен для чтения
func (c consumerHeaderCarrier) Keys() []string {
	keys := make([]string, len(c.headers))
	for i, h := range c.headers {
		keys[i] = string(h.Key)
	}
	return keys
}

// ConsumerTracing — это твой middleware для склейки трейсов
func ConsumerTracing() kafka.Middleware {
	return func(next kafka.MessageHandler) kafka.MessageHandler {
		return func(ctx context.Context, ev kafka.ConsumeMessage) error {

			// 1. Извлекаем traceparent из заголовков сообщения
			carrier := consumerHeaderCarrier{headers: ev.Headers()}
			extractedCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)

			// 2. Создаем спан консюмера.
			// extractedCtx теперь знает о спане продюсера!
			ctxWithSpan, span := otel.Tracer("kafka.consumer").Start(
				extractedCtx,
				fmt.Sprintf("%s process", ev.Topic()),
				trace.WithSpanKind(trace.SpanKindConsumer),
			)
			defer span.End()

			// 3. Передаем контекст СО СПАНОМ дальше в бизнес-логику
			return next(ctxWithSpan, ev)
		}
	}
}

```
