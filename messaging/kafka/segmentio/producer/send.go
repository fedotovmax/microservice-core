package producer

import (
	"context"
	"fmt"

	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/messaging/kafka/segmentio"
	skafka "github.com/segmentio/kafka-go"
)

func (p *producer) Send(ctx context.Context, event kafka.Message) error {
	const op = "core.messaging.kafka.segmentio.producer.Send"

	msg := skafka.Message{
		Topic:      event.Topic(),
		Key:        []byte(event.Key()),
		Value:      event.Payload(),
		WriterData: event.Meta(),
		Headers:    segmentio.HeadersToSegmentio(event.Headers()),
	}

	fmt.Printf("PRODUCER HEADERS: %+v\n", msg.Headers)

	// WriteMessages в Async режиме не блокируется дольше, чем нужно на запись в буфер
	err := p.w.WriteMessages(ctx, msg)
	if err != nil {
		return fmt.Errorf("%s: error writing message: %w", op, err)
	}

	return nil
}
