package consumer

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/messaging/kafka/middlewares"
	coreSarama "github.com/fedotovmax/microservice-core/messaging/kafka/sarama"
	"github.com/fedotovmax/microservice-core/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

type groupHandler struct {
	handler           kafka.MessageHandler
	onCleanUp         kafka.OnCleanUp
	onSetup           kafka.OnSetup
	maxProcessingTime time.Duration
	log               logger.Logger
	onMark            func(msg *sarama.ConsumerMessage, metadata string)
}

func newGroupHandler(
	log logger.Logger,
	p kafka.ConsumerGroupStartReadParams,
	maxProcTime time.Duration,
	tracing bool,
) sarama.ConsumerGroupHandler {

	h := p.MessageHandler

	// 1. Применяем пользовательские мидлвари (накручиваем внутренние слои)
	for i := len(p.Middlewares) - 1; i >= 0; i-- {
		h = p.Middlewares[i](h)
	}

	var onMark func(msg *sarama.ConsumerMessage, metadata string)

	// 2. Оборачиваем в НАШ трейсинг (он будет самым внешним и первым перехватит контекст)
	if tracing {

		h = middlewares.ConsumerTracingMiddleware()(h)

		onMark = func(msg *sarama.ConsumerMessage, metadata string) {
			msgCtx := otel.GetTextMapPropagator().Extract(context.Background(), msgSaramaCarrier(msg.Headers))
			_, span := otel.Tracer(kafka.TracerName).Start(msgCtx, kafka.TraceConsumerHandleMark,
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					semconv.MessagingSystemKey.String(kafka.TraceSystemKey),
					semconv.MessagingDestinationName(msg.Topic),
					semconv.MessagingMessageIDKey.String(strconv.FormatInt(msg.Offset, 10)),
					semconv.MessagingKafkaMessageKeyKey.String(string(msg.Key)),
					semconv.MessagingKafkaDestinationPartitionKey.Int64(int64(msg.Partition)),
				),
			)
			if len(msg.Headers) > 0 {
				headerAttrs := make([]attribute.KeyValue, 0, len(msg.Headers))

				for _, h := range msg.Headers {
					key := string(h.Key)
					if key == observability.TraceParent {
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

	return &groupHandler{
		handler:           h,
		onCleanUp:         p.OnCleanUp,
		onSetup:           p.OnSetup,
		maxProcessingTime: maxProcTime,
		log:               log,
		onMark:            onMark,
	}

}

func (h *groupHandler) Setup(s sarama.ConsumerGroupSession) error {
	if h.onSetup != nil {
		return h.onSetup()
	}

	return nil

}

func (h *groupHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	if h.onCleanUp != nil {
		return h.onCleanUp()
	}

	return nil
}

func (h *groupHandler) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {

	const op = "core.messaging.kafka.sarama.consumer.groupHandler.ConsumeClaim"

	log := h.log.With(logger.String("op", op))

	markMessage := func(msg *sarama.ConsumerMessage, info string) {
		if h.onMark != nil {
			h.onMark(msg, info)
		}
		s.MarkMessage(msg, info)
	}

	for {
		select {

		case <-s.Context().Done():

			log.Warn("session context is done, stopping messages handling")
			return nil

		case msg, ok := <-c.Messages():

			if !ok {
				log.Warn("messages channel was closed, stopping messages handling")
				return nil
			}

			payload := msg.Value

			ev := kafka.NewConsumeMessage(payload, msg.Key, msg.Offset, msg.Topic, msg.Partition, coreSarama.HeadersFromPtrSarama(msg.Headers))

			if err := h.handle(s.Context(), ev); err != nil {
				if noRetryErr, ok := errors.AsType[*kafka.NoRetryError](err); ok {
					markMessage(msg, noRetryErr.Reason)
					continue
				}
				log.Error("an unexpected error was received while processing the message", logger.Err(err))
				continue
			}

			markMessage(msg, "")
		}
	}
}

func (h *groupHandler) handle(ctx context.Context, ev kafka.ConsumeMessage) error {
	readctx, cancel := context.WithTimeout(ctx, h.maxProcessingTime)
	defer cancel()
	return h.handler(readctx, ev)

}
