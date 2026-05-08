package consumer

import (
	"context"
	"errors"
	"time"

	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/messaging/kafka/middlewares"
	coreSarama "github.com/fedotovmax/microservice-core/messaging/kafka/sarama"
)

type groupHandler struct {
	handler           kafka.MessageHandler
	onCleanUp         kafka.OnCleanUp
	onSetup           kafka.OnSetup
	maxProcessingTime time.Duration
	log               logger.Logger
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

	// 2. Оборачиваем в НАШ трейсинг (он будет самым внешним и первым перехватит контекст)
	if tracing {
		h = middlewares.ConsumerTracingMiddleware()(h)
	}

	return &groupHandler{
		handler:           h,
		onCleanUp:         p.OnCleanUp,
		onSetup:           p.OnSetup,
		maxProcessingTime: maxProcTime,
		log:               log,
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
					s.MarkMessage(msg, noRetryErr.Reason)
					continue
				}
				log.Error("an unexpected error was received while processing the message", logger.Err(err))
				continue
			}

			s.MarkMessage(msg, "")
		}
	}
}

func (h *groupHandler) handle(ctx context.Context, ev kafka.ConsumeMessage) error {
	readctx, cancel := context.WithTimeout(ctx, h.maxProcessingTime)
	defer cancel()
	return h.handler(readctx, ev)

}
