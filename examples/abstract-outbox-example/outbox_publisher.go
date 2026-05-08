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

func kafkaHeadersToOutbox(kh []kafka.Header) []outbox.Header {
	outboxHeaders := make([]outbox.Header, 0, len(kh))

	for _, h := range kh {
		outboxHeaders = append(outboxHeaders, outbox.Header{
			Key:   h.Key,
			Value: h.Value,
		})
	}

	return outboxHeaders
}

func outboxHeadersToKafka(oh []outbox.Header) []kafka.Header {
	kafkaHeaders := make([]kafka.Header, 0, len(oh))

	for _, h := range oh {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   h.Key,
			Value: h.Value,
		})
	}

	return kafkaHeaders
}

func (p *PublisherAdapter) HandleErrors(timeout time.Duration, onError outbox.OnError) {

	p.AsyncProducer.HandleErrors(
		timeout,
		func(ctx context.Context, event kafka.FailedMessage) error {
			return onError(
				ctx,
				outbox.NewFailedEvent(
					event.Meta(),
					kafkaHeadersToOutbox(event.Headers()),
					event.Error(),
				),
			)
		},
	)
}

func (p *PublisherAdapter) HandleSuccesses(timeout time.Duration, OnSuccess outbox.OnSuccess) {

	p.AsyncProducer.HandleSuccesses(
		timeout,
		func(ctx context.Context, event kafka.SuccessMessage) error {
			return OnSuccess(
				ctx,
				outbox.NewSuccessEvent(
					event.Meta(),
					kafkaHeadersToOutbox(event.Headers()),
				),
			)
		},
	)
}

func (p *PublisherAdapter) Send(ctx context.Context, ev outbox.Event) error {
	return p.AsyncProducer.Send(
		ctx,
		kafka.NewMessage(ev.RoutingKey(), ev.Destination(), ev.Payload(), outboxHeadersToKafka(ev.Headers()), ev.InternalMeta()),
	)
}
