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

type onMarkFunc func(msg *sarama.ConsumerMessage, metadata string)

type groupHandler struct {
	handler           kafka.MessageHandler
	onCleanUp         kafka.OnCleanUp
	onSetup           kafka.OnSetup
	maxProcessingTime time.Duration
	log               logger.Logger
	onMark            onMarkFunc
	withMetrics       bool
}

func newGroupHandler(
	log logger.Logger,
	p kafka.ConsumerGroupStartReadParams,
	maxProcTime time.Duration,
	telemetry bool,
) sarama.ConsumerGroupHandler {

	p.MessageHandler = kafka.ChainMiddlewares(p.MessageHandler, p.Middlewares...)

	var onMark onMarkFunc
	var withMetrics bool

	// 2. Оборачиваем в НАШ трейсинг (он будет самым внешним и первым перехватит контекст)
	if telemetry {

		p.MessageHandler = middlewares.ConsumerTelemetryMiddleware(telemetry)(p.MessageHandler)

		onMark = createOnMark()

		err := kafka.InitConsumerInfraMetrics()
		if err != nil {
			log.Error("failed to init consumer metrics", logger.Err(err))
		} else {
			withMetrics = true
		}
	}

	return &groupHandler{
		handler:           p.MessageHandler,
		onCleanUp:         p.OnCleanUp,
		onSetup:           p.OnSetup,
		maxProcessingTime: maxProcTime,
		log:               log,
		onMark:            onMark,
		withMetrics:       withMetrics,
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

// TODO: use metrics for something...
func (h *groupHandler) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {

	const op = "core.messaging.kafka.sarama.consumer.groupHandler.ConsumeClaim"

	log := h.log.With(logger.String("op", op))

	markMessage := func(msg *sarama.ConsumerMessage, info string) {
		if h.onMark != nil {
			h.onMark(msg, info)
		}
		s.MarkMessage(msg, info)
	}

	ctx := s.Context()

	for {
		select {

		case <-ctx.Done():

			log.Warn("session context is done, stopping messages handling")
			return nil

		case msg, ok := <-c.Messages():

			if !ok {
				log.Warn("messages channel was closed, stopping messages handling")
				return nil
			}

			payload := msg.Value

			ev := kafka.NewConsumeMessage(payload, msg.Key, msg.Offset, msg.Topic, msg.Partition, coreSarama.HeadersFromPtrSarama(msg.Headers))

			if err := h.handle(ctx, ev); err != nil {
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
