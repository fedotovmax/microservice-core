package producer

import (
	"context"
	"fmt"
	"strconv"

	skafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"go.opentelemetry.io/otel/trace"
)

type messageCarrier struct {
	msg *skafka.Message
}

func (c messageCarrier) Get(key string) string {
	for _, h := range c.msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c messageCarrier) Set(key, value string) {
	c.msg.Headers = append(c.msg.Headers, skafka.Header{
		Key:   key,
		Value: []byte(value),
	})
}

func (c messageCarrier) Keys() []string {
	keys := make([]string, len(c.msg.Headers))
	for i, h := range c.msg.Headers {
		keys[i] = h.Key
	}
	return keys
}

type otelWriter struct {
	w *skafka.Writer
}

func newOtelWriter(w *skafka.Writer) *otelWriter {
	return &otelWriter{w: w}
}

func (o *otelWriter) startSpan(ctx context.Context, msg *skafka.Message) trace.Span {
	ctx, span := otel.Tracer("kafka.producer").Start(ctx,
		fmt.Sprintf("%s send", msg.Topic),
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			semconv.MessagingSystemKey.String("kafka"),
			semconv.MessagingDestinationName(msg.Topic),
			semconv.MessagingMessageIDKey.String(strconv.FormatInt(msg.Offset, 10)),
			semconv.MessagingKafkaMessageKeyKey.String(string(msg.Key)),
			semconv.MessagingKafkaDestinationPartitionKey.Int64(int64(msg.Partition)),
		),
	)

	otel.GetTextMapPropagator().Inject(ctx, messageCarrier{msg: msg})

	return span
}

func (o *otelWriter) WriteMessage(ctx context.Context, msg skafka.Message) error {
	span := o.startSpan(ctx, &msg)
	defer span.End()

	if err := o.w.WriteMessages(ctx, msg); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

func (o *otelWriter) WriteMessages(ctx context.Context, msgs ...skafka.Message) error {
	spans := make([]trace.Span, len(msgs))
	for i := range msgs {
		spans[i] = o.startSpan(ctx, &msgs[i])
	}
	defer func() {
		for _, span := range spans {
			span.End()
		}
	}()

	if err := o.w.WriteMessages(ctx, msgs...); err != nil {
		for _, span := range spans {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}

	return nil
}

func (o *otelWriter) Close() error {
	return o.w.Close()
}
