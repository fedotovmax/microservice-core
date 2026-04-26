package producer

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	coreSarama "github.com/fedotovmax/microservice-core/messaging/kafka/sarama"
)

func (p *producer) Send(ctx context.Context, event kafka.Event) error {

	const op = "core.messaging.kafka.sarama.producer.Send"

	msg := &sarama.ProducerMessage{
		Topic:    event.Topic(),
		Key:      sarama.StringEncoder(event.Key()),
		Value:    sarama.ByteEncoder(event.Payload()),
		Headers:  coreSarama.HeadersToSarama(event.Headers()),
		Metadata: event.Meta(),
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf(
			"%s: cannot send event with key: %s; headers: %v, context is done: %w",
			op, event.Key(),
			event.Headers(),
			ctx.Err(),
		)
	case p.ap.Input() <- msg:
		return nil
	}
}
