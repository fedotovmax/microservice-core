package main

import (
	"context"
	"time"

	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/outbox"
)

type PublisherAdapter struct {
	kafka.AsyncProducer
}

func (p *PublisherAdapter) HandleErrors(timeout time.Duration, onError outbox.OnError) {

	p.AsyncProducer.HandleErrors(
		timeout,
		func(ctx context.Context, event kafka.FailedMessage) error {
			return onError(
				ctx,
				outbox.NewFailedEvent(
					outbox.NewEvent(
						event.Message().Key(),
						event.Message().Topic(),
						event.Message().Payload(),
						outbox.HeadersFromKafka(event.Message().Headers()),
						event.Message().Meta(),
					),
					event.Error(),
				),
			)
		},
	)
}

func (p *PublisherAdapter) HandleSuccesses(timeout time.Duration, onSuccess outbox.OnSuccess) {

	p.AsyncProducer.HandleSuccesses(
		timeout,
		func(ctx context.Context, event kafka.Message) error {
			return onSuccess(
				ctx,
				outbox.NewEvent(
					event.Key(),
					event.Topic(),
					event.Payload(),
					outbox.HeadersFromKafka(event.Headers()),
					event.Meta(),
				),
			)
		},
	)
}

func (p *PublisherAdapter) Send(ctx context.Context, ev outbox.Event) error {
	return p.AsyncProducer.Send(
		ctx,
		kafka.NewMessage(ev.RoutingKey(), ev.Destination(), ev.Payload(), outbox.HeadersToKafka(ev.Headers()), ev.InternalMeta()),
	)
}
