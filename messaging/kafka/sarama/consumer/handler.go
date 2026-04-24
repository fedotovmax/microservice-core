package consumer

import (
	"context"
	"errors"
	"time"

	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	coreSarama "github.com/fedotovmax/microservice-core/messaging/kafka/sarama"
)

type groupHandler struct {
	handler           kafka.MessageHandler
	onCleanUp         kafka.OnCleanUp
	onSetup           kafka.OnSetup
	maxProcessingTime time.Duration
	log               logger.Logger
}

func NewGroupHandler(
	log logger.Logger,
	p kafka.ConsumerGroupStartReadParams,
	maxProcTime time.Duration,
) *groupHandler {

	for i := len(p.Middlewares) - 1; i >= 0; i-- {
		p.MessageHandler = p.Middlewares[i](p.MessageHandler)
	}

	return &groupHandler{
		handler:           p.MessageHandler,
		onCleanUp:         p.OnCleanUp,
		onSetup:           p.OnSetup,
		maxProcessingTime: maxProcTime,
		log:               log,
	}

}

func (h *groupHandler) Setup(s sarama.ConsumerGroupSession) error {
	return h.onSetup()

}

func (h *groupHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	return h.onCleanUp()
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

			ev := kafka.NewConsumeEvent(payload, msg.Key, msg.Offset, msg.Topic, msg.Partition, coreSarama.SaramaPtrHeadersToCore(msg.Headers))

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

func (h *groupHandler) handle(ctx context.Context, ev kafka.ConsumeEvent) error {
	readctx, cancel := context.WithTimeout(ctx, h.maxProcessingTime)
	defer cancel()
	return h.handler(readctx, ev)

}
