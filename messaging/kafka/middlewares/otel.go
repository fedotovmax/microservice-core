package middlewares

import (
	"context"
	"fmt"
	"strconv"

	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

type consumerHandlerHeadersCarrier []kafka.Header

func (c consumerHandlerHeadersCarrier) Get(key string) string {
	for _, h := range c {
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c consumerHandlerHeadersCarrier) Set(k, v string) {} // Set не нужен для консюмера

func (c consumerHandlerHeadersCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for _, h := range c {
		keys = append(keys, string(h.Key)) // Небольшой фикс: безопаснее делать append
	}
	return keys
}

func ConsumerTracingMiddleware() kafka.Middleware {
	return func(next kafka.MessageHandler) kafka.MessageHandler {
		return func(ctx context.Context, ev kafka.ConsumeMessage) error {
			// 1. Извлекаем контекст продюсера из заголовков
			headers := ev.Headers()

			carrier := consumerHandlerHeadersCarrier(headers)
			extractedCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)

			// 2. Создаем спан обработки сообщения
			ctxWithSpan, span := otel.Tracer(kafka.TracerName).Start(
				extractedCtx,
				fmt.Sprintf("%s process", ev.Topic()),
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					semconv.MessagingSystemKey.String(kafka.TraceSystemKey),
					semconv.MessagingDestinationName(ev.Topic()),
					semconv.MessagingMessageIDKey.String(strconv.FormatInt(ev.Offset(), 10)),
					semconv.MessagingKafkaMessageKeyKey.String(string(ev.Key())),
					semconv.MessagingKafkaDestinationPartitionKey.Int64(int64(ev.Partition())),
				),
			)
			defer span.End()

			if len(headers) > 0 {
				headerAttrs := make([]attribute.KeyValue, 0, len(headers))

				for _, h := range headers {
					key := string(h.Key)
					if key == observability.TraceParent {
						continue
					}
					attrKey := kafka.TraceHeaderKey(key)
					headerAttrs = append(headerAttrs, attribute.String(attrKey, string(h.Value)))
				}
				span.SetAttributes(headerAttrs...)
			}

			// 3. Вызываем бизнес-логику с новым контекстом
			err := next(ctxWithSpan, ev)

			// 4. Если бизнес-логика вернула ошибку, красим спан в красный
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}

			return err
		}
	}
}
