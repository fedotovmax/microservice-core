package producer

import (
	"context"
	"errors"
	"strconv"

	"github.com/fedotovmax/microservice-core/messaging/kafka"
	skafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

// msgWriterCarrier для проброса traceparent в заголовки Kafka
type msgWriterCarrier struct {
	msg *skafka.Message
}

func (c msgWriterCarrier) Get(k string) string {
	for _, h := range c.msg.Headers {
		if h.Key == k {
			return string(h.Value)
		}
	}
	return ""
}

func (c msgWriterCarrier) Set(k, v string) {
	c.msg.Headers = append(c.msg.Headers, skafka.Header{Key: k, Value: []byte(v)})
}
func (c msgWriterCarrier) Keys() (keys []string) {
	for _, h := range c.msg.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}

// spanWrapper прячет спан и твои оригинальные данные (WriterData)
type spanWrapper struct {
	original interface{}
	span     trace.Span
}

type tracedWriter struct {
	w      *skafka.Writer
	tracer trace.Tracer
}

// NewOtelWriter перехватывает Completion для закрытия спанов
func newTracedWriter(w *skafka.Writer) *tracedWriter {
	origCompletion := w.Completion

	w.Completion = func(msgs []skafka.Message, err error) {
		var wErrs skafka.WriteErrors
		isWErrs := errors.As(err, &wErrs)

		for i := range msgs {
			if sw, ok := msgs[i].WriterData.(spanWrapper); ok {
				span := sw.span

				// 1. Записываем ошибки, если они есть
				if err != nil {
					e := err
					if isWErrs && wErrs[i] != nil {
						e = wErrs[i]
					}
					if !isWErrs || wErrs[i] != nil {
						span.RecordError(e)
						span.SetStatus(codes.Error, e.Error())
					}
				}

				// 2. Фиксируем реальные партиции и оффсеты, закрываем спан
				span.SetAttributes(
					semconv.MessagingMessageIDKey.String(strconv.FormatInt(msgs[i].Offset, 10)),
					semconv.MessagingKafkaDestinationPartitionKey.Int64(int64(msgs[i].Partition)),
				)
				span.End()

				// 3. Восстанавливаем твои оригинальные данные
				msgs[i].WriterData = sw.original
			}
		}

		// 4. Вызываем твой бизнес-код с каналами
		if origCompletion != nil {
			origCompletion(msgs, err)
		}
	}

	return &tracedWriter{w: w, tracer: otel.Tracer(kafka.TracerName)}
}

func (o *tracedWriter) WriteMessages(ctx context.Context, msgs ...skafka.Message) error {
	cp := make([]skafka.Message, len(msgs)) // Копируем, чтобы не портить оригинал

	for i, msg := range msgs {
		topic := msg.Topic
		if topic == "" {
			topic = o.w.Topic
		}

		// 1. Формируем базовые атрибуты
		spanAttrs := []attribute.KeyValue{
			semconv.MessagingSystemKey.String("kafka"),
			semconv.MessagingDestinationName(topic),
			semconv.MessagingKafkaMessageKeyKey.String(string(msg.Key)),
		}

		// 2. ВОТ ЭТОТ ЦИКЛ: перекладываем бизнес-заголовки в атрибуты спана
		for _, h := range msg.Headers {
			attrKey := "messaging.kafka.header." + string(h.Key)
			spanAttrs = append(spanAttrs, attribute.String(attrKey, string(h.Value)))
		}

		// 3. Стартуем спан с собранными атрибутами (используем единое имя трейсера)
		ctx, span := o.tracer.Start(ctx, topic+" send",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(spanAttrs...),
		)

		// 4. Инжектим traceparent в заголовки Segmentio
		// Здесь как раз отрабатывает msgCarrier.Set, добавляя новый header
		otel.GetTextMapPropagator().Inject(ctx, msgWriterCarrier{msg: &msg})

		// 5. Оборачиваем данные и спан для коллбека Completion
		msg.WriterData = spanWrapper{
			original: msg.WriterData,
			span:     span,
		}
		cp[i] = msg
	}

	return o.w.WriteMessages(ctx, cp...)
}

func (o *tracedWriter) Close() error {
	return o.w.Close()
}
