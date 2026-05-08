package consumer

import (
	"context"

	"github.com/fedotovmax/microservice-core/messaging/kafka"
	skafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type msgReaderCarrier []skafka.Header

func (c msgReaderCarrier) Get(key string) string {
	for _, h := range c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
func (c msgReaderCarrier) Set(k, v string) {}
func (c msgReaderCarrier) Keys() []string  { return nil }

// tracedReader оборачивает любой segmentioReader и добавляет трейсинг инфраструктурных вызовов
type tracedReader struct {
	base   segmentioReader
	tracer trace.Tracer
}

func newTracedReader(base segmentioReader) *tracedReader {
	return &tracedReader{
		base:   base,
		tracer: otel.Tracer(kafka.TracerName),
	}
}

func (t *tracedReader) FetchMessage(ctx context.Context) (skafka.Message, error) {
	// Fetch не трейсим отдельным спаном, чтобы не плодить сиротские трейсы без TraceID
	return t.base.FetchMessage(ctx)
}

func (t *tracedReader) CommitMessages(ctx context.Context, msgs ...skafka.Message) error {

	// Магия сшивки: достаем контекст прямо из коммитящегося сообщения!
	if len(msgs) > 0 {
		ctx = otel.GetTextMapPropagator().Extract(ctx, msgReaderCarrier(msgs[0].Headers))
	}

	// Теперь этот спан будет дочерним для Продюсера (или братом для консюмера)
	ctx, span := t.tracer.Start(ctx, "kafka commit", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	err := t.base.CommitMessages(ctx, msgs...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return err
}

func (t *tracedReader) Close() error {
	return t.base.Close()
}
