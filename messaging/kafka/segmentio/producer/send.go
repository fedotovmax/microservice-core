package producer

import (
	"context"
	"fmt"
	"time"

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

	withTracer := p.withTracer()

	// 2. Если трейсинг включен — дорабатываем сообщение
	if withTracer {
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
		smsg.WriterData = kafka.TelemetryMetaWrapper{
			Original:  kmsg.Meta(),
			Span:      span,
			StartTime: time.Now(),
		}
	}

	// WriteMessages в Async режиме не блокируется дольше, чем нужно на запись в буфер
	err := p.w.WriteMessages(ctx, smsg)
	if err != nil {
		if withTracer {
			if wrapper, ok := smsg.WriterData.(kafka.TelemetryMetaWrapper); ok {
				wrapper.Span.RecordError(err)
				wrapper.Span.SetStatus(codes.Error, err.Error())
				wrapper.Span.End()
			}

		}

		return fmt.Errorf("%s: error writing message: %w", op, err)
	}

	return nil
}
