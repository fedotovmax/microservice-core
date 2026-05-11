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

func (p *producer) Send(ctx context.Context, msg kafka.Message) error {
	const op = "core.messaging.kafka.segmentio.producer.Send"

	smsg := skafka.Message{
		Topic:      msg.Topic(),
		Key:        []byte(msg.Key()),
		Value:      msg.Payload(),
		WriterData: msg.Meta(),
		Headers:    segmentio.HeadersToSegmentio(msg.Headers()),
	}

	withTracing := p.withTracing()

	// 2. Если трейсинг включен — дорабатываем сообщение
	if withTracing {
		spanAttrs := []attribute.KeyValue{
			semconv.MessagingSystemKey.String(kafka.TraceSystemKey),
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
		otel.GetTextMapPropagator().Inject(ctx, msgWriterCarrier{msg: &smsg})

		// 4. Перезаписываем поле Metadata на нашу матрешку
		smsg.WriterData = kafka.TelemetryMetaWrapper{
			Original:  msg.Meta(),
			Span:      span,
			StartTime: time.Now(),
		}
	}

	// WriteMessages в Async режиме не блокируется дольше, чем нужно на запись в буфер
	err := p.w.WriteMessages(ctx, smsg)
	if err != nil {
		if withTracing {
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
