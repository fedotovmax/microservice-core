package middlewares

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/telemetry"
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

		metrics, err := kafka.NewConsumerMetrics()
		if err != nil {
			metrics = nil
		}

		return func(ctx context.Context, msg kafka.ConsumeMessage) error {
			// 1. Извлекаем контекст продюсера из заголовков
			headers := msg.Headers()

			carrier := consumerHandlerHeadersCarrier(headers)
			extractedCtx := otel.GetTextMapPropagator().Extract(ctx, carrier)

			// 2. Создаем спан обработки сообщения

			ctxWithSpan, span := telemetry.StartPlatformSpan(
				extractedCtx,
				kafka.PlatformTelemetryConsumer,
				kafka.TraceConsumeTopic(msg.Topic()),
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					semconv.MessagingSystemKey.String(kafka.TraceSystemKey),
					semconv.MessagingDestinationName(msg.Topic()),
					semconv.MessagingMessageIDKey.String(strconv.FormatInt(msg.Offset(), 10)),
					semconv.MessagingKafkaMessageKeyKey.String(string(msg.Key())),
					semconv.MessagingKafkaDestinationPartitionKey.Int64(int64(msg.Partition())),
				))

			defer span.End()

			if len(headers) > 0 {
				headerAttrs := make([]attribute.KeyValue, 0, len(headers))

				for _, h := range headers {
					key := string(h.Key)
					if key == telemetry.TraceParent {
						continue
					}
					attrKey := kafka.TraceHeaderKey(key)
					headerAttrs = append(headerAttrs, attribute.String(attrKey, string(h.Value)))
				}
				span.SetAttributes(headerAttrs...)
			}

			// 3. Вызываем бизнес-логику с новым контекстом
			start := time.Now()
			err := next(ctxWithSpan, msg)
			elapsedSeconds := time.Since(start).Seconds() // Секунды вместо миллисекунд

			status := kafka.MetricsConsumerHandlerStatusSuccess
			errType := kafka.MetricsConsumerHandlerErrorNone

			// 4. Если бизнес-логика вернула ошибку, красим спан в красный
			if err != nil {
				status = kafka.MetricsConsumerHandlerStatusError
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())

				if _, ok := errors.AsType[*kafka.NoRetryError](err); ok {
					errType = kafka.MetricsConsumerHandlerErrorNoRetry
				} else {
					errType = kafka.MetricsConsumerHandlerErrorRetryable
				}
			}

			if metrics != nil {
				metrics.RecordMessage(ctxWithSpan, msg.Topic(), status, errType)
				metrics.RecordProcessingTime(ctxWithSpan, msg.Topic(), elapsedSeconds)
			}
			return nil
		}
	}
}
