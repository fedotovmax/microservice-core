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

func (p *producer) Send(ctx context.Context, msg kafka.Message) error {
	const op = "core.messaging.kafka.sarama.producer.Send"

	// 1. Сразу собираем базовое сообщение с твоими заголовками
	smsg := &sarama.ProducerMessage{
		Topic:    msg.Topic(),
		Key:      sarama.StringEncoder(msg.Key()),
		Value:    sarama.ByteEncoder(msg.Payload()),
		Headers:  coreSarama.HeadersToSarama(msg.Headers()),
		Metadata: msg.Meta(), // Пока кладем оригинальную метадату
	}

	withTracing := p.withTracing()

	// 2. Если трейсинг включен — дорабатываем сообщение
	if withTracing {
		spanAttrs := []attribute.KeyValue{
			semconv.MessagingSystemKey.String(kafka.TelemetryKey),
			semconv.MessagingDestinationName(msg.Topic()),
			semconv.MessagingKafkaMessageKeyKey.String(string(msg.Key())),
		}

		for _, h := range msg.Headers() {
			spanAttrs = append(spanAttrs, attribute.String(kafka.TraceHeaderKey(string(h.Key)), string(h.Value)))
		}

		var span trace.Span
		ctx, span = p.tracer.Start(ctx, kafka.TraceProducerSendTopic(msg.Topic()),
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(spanAttrs...),
		)

		// 3. Инжектим прямо в структуру сообщения!
		otel.GetTextMapPropagator().Inject(ctx, saramaMsgCarrier{msg: smsg})

		// 4. Перезаписываем поле Metadata на нашу матрешку
		smsg.Metadata = kafka.TelemetryMetaWrapper{
			Original:  msg.Meta(),
			Span:      span,
			StartTime: time.Now(),
		}
	}

	// Отправляем в канал...
	select {
	case <-ctx.Done():
		if withTracing {
			if wrapper, ok := smsg.Metadata.(kafka.TelemetryMetaWrapper); ok {
				wrapper.Span.RecordError(ctx.Err())
				wrapper.Span.SetStatus(codes.Error, ctx.Err().Error())
				wrapper.Span.End()
			}
		}
		return fmt.Errorf("%s: cannot send event with key: %s; context is done: %w", op, msg.Key(), ctx.Err())

	case p.ap.Input() <- smsg:
		return nil
	}
}
