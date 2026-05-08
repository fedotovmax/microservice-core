package producer

import (
	"context"
	"strconv"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/messaging/kafka/sarama"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

func (p *producer) HandleSuccesses(timeout time.Duration, onSuccess kafka.OnSuccess) {
	const op = "core.messaging.kafka.sarama.producer.HandleSuccesses"
	log := p.log.With(logger.String("op", op))

	for msg := range p.ap.Successes() {
		meta := msg.Metadata

		// Распаковываем спан и оригинальную метадату
		if wrapper, ok := meta.(spanWrapper); ok {
			meta = wrapper.original

			wrapper.span.SetAttributes(
				semconv.MessagingMessageIDKey.String(strconv.FormatInt(msg.Offset, 10)),
				semconv.MessagingKafkaDestinationPartitionKey.Int64(int64(msg.Partition)),
			)
			wrapper.span.End() // Успешно закрываем спан!
		}

		successMsg, err := sarama.NewMessageFromProducer(msg, meta)

		if err != nil {
			log.Error("error when encode producer message to domain kafka message", logger.Err(err))
			continue
		}

		if err := p.handleSuccess(successMsg, timeout, onSuccess); err != nil {
			log.Error("error when call onSuccess callback", logger.Err(err))
		}
	}
}
func (p *producer) handleSuccess(e kafka.Message, timeout time.Duration, onSuccess kafka.OnSuccess) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return onSuccess(ctx, e)
}
