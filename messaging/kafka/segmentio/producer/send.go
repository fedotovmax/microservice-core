package producer

import (
	"context"
	"fmt"

	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/messaging/kafka/segmentio"
	skafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

func (p *producer) Send(ctx context.Context, kmsg kafka.Message) error {
	const op = "core.messaging.kafka.segmentio.producer.Send"

	smsg := skafka.Message{
		Topic:      kmsg.Topic(),
		Key:        []byte(kmsg.Key()),
		Value:      kmsg.Payload(),
		WriterData: kmsg.Meta(),
		Headers:    segmentio.HeadersToSegmentio(kmsg.Headers()),
	}

	// 2. Если трейсинг включен — дорабатываем сообщение
	if p.tracer != nil {
		spanAttrs := []attribute.KeyValue{
			semconv.MessagingSystemKey.String(kafka.TraceSystemKey),
			semconv.MessagingDestinationName(kmsg.Topic()),
			semconv.MessagingKafkaMessageKeyKey.String(string(kmsg.Key())),
		}

		for _, h := range kmsg.Headers() {
			spanAttrs = append(spanAttrs, attribute.String(kafka.TraceHeaderKey(string(h.Key)), string(h.Value)))
		}

		var span trace.Span
		ctx, span = p.tracer.Start(ctx, kafka.TraceProducerSendTopic(kmsg.Topic()),
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(spanAttrs...),
		)

		// 3. Инжектим прямо в структуру сообщения!
		otel.GetTextMapPropagator().Inject(ctx, msgWriterCarrier{msg: &smsg})

		// 4. Перезаписываем поле Metadata на нашу матрешку
		smsg.WriterData = kafka.SpanMetaWrapper{
			Original: kmsg.Meta(),
			Span:     span,
		}
	}

	// WriteMessages в Async режиме не блокируется дольше, чем нужно на запись в буфер
	err := p.w.WriteMessages(ctx, smsg)
	if err != nil {
		if p.tracer != nil {
			if wrapper, ok := smsg.WriterData.(kafka.SpanMetaWrapper); ok {
				wrapper.Span.RecordError(ctx.Err())
				wrapper.Span.SetStatus(codes.Error, ctx.Err().Error())
				wrapper.Span.End()
			}
		}

		return fmt.Errorf("%s: error writing message: %w", op, err)
	}

	return nil
}
