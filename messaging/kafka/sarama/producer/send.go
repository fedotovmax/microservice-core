package producer

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	coreSarama "github.com/fedotovmax/microservice-core/messaging/kafka/sarama"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

func (p *producer) Send(ctx context.Context, event kafka.Message) error {
	const op = "core.messaging.kafka.sarama.producer.Send"

	// 1. Сразу собираем базовое сообщение с твоими заголовками
	msg := &sarama.ProducerMessage{
		Topic:    event.Topic(),
		Key:      sarama.StringEncoder(event.Key()),
		Value:    sarama.ByteEncoder(event.Payload()),
		Headers:  coreSarama.HeadersToSarama(event.Headers()),
		Metadata: event.Meta(), // Пока кладем оригинальную метадату
	}

	withTracer := p.withTracer()

	// 2. Если трейсинг включен — дорабатываем сообщение
	if withTracer {
		spanAttrs := []attribute.KeyValue{
			semconv.MessagingSystemKey.String(kafka.TraceSystemKey),
			semconv.MessagingDestinationName(event.Topic()),
			semconv.MessagingKafkaMessageKeyKey.String(string(event.Key())),
		}

		for _, h := range event.Headers() {
			spanAttrs = append(spanAttrs, attribute.String(kafka.TraceHeaderKey(string(h.Key)), string(h.Value)))
		}

		var span trace.Span
		ctx, span = p.tracer.Start(ctx, event.Topic()+" send",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(spanAttrs...),
		)

		// 3. Инжектим прямо в структуру сообщения!
		otel.GetTextMapPropagator().Inject(ctx, saramaMsgCarrier{msg: msg})

		// 4. Перезаписываем поле Metadata на нашу матрешку
		msg.Metadata = kafka.TelemetryMetaWrapper{
			Original:  event.Meta(),
			Span:      span,
			StartTime: time.Now(),
		}
	}

	// Отправляем в канал...
	select {
	case <-ctx.Done():
		if withTracer {
			if wrapper, ok := msg.Metadata.(kafka.TelemetryMetaWrapper); ok {
				wrapper.Span.RecordError(ctx.Err())
				wrapper.Span.SetStatus(codes.Error, ctx.Err().Error())
				wrapper.Span.End()
			}
		}
		return fmt.Errorf("%s: cannot send event with key: %s; context is done: %w", op, event.Key(), ctx.Err())

	case p.ap.Input() <- msg:
		return nil
	}
}
