package consumer

import (
	"context"
	"strconv"

	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/telemetry"
	skafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

func createOnMark(ctx context.Context, tracer trace.Tracer) onMarkFunc {
	return func(msg skafka.Message) {
		if tracer != nil {
			msgCtx := otel.GetTextMapPropagator().Extract(ctx, msgReaderCarrier(msg.Headers))

			_, span := tracer.Start(
				msgCtx,
				kafka.TraceConsumerHandleMark(msg.Topic),
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					semconv.MessagingSystemKey.String(kafka.TelemetryKey),
					semconv.MessagingDestinationName(msg.Topic),
					semconv.MessagingMessageIDKey.String(strconv.FormatInt(msg.Offset, 10)),
					semconv.MessagingKafkaMessageKeyKey.String(string(msg.Key)),
					semconv.MessagingKafkaDestinationPartitionKey.Int64(int64(msg.Partition)),
				))

			if len(msg.Headers) > 0 {
				headerAttrs := make([]attribute.KeyValue, 0, len(msg.Headers))

				for _, h := range msg.Headers {
					key := string(h.Key)
					if key == telemetry.TraceParent {
						continue
					}
					attrKey := kafka.TraceHeaderKey(key)
					headerAttrs = append(headerAttrs, attribute.String(attrKey, string(h.Value)))
				}
				span.SetAttributes(headerAttrs...)
			}
			span.End()
		}
	}
}
