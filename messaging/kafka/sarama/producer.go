package sarama

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

type Producer struct {
	sarama.AsyncProducer
}

func NewProducer(config ProducerConfig) (kafka.AsyncProducer, error) {
	const op = "core.messaging.kafka.sarama.NewProducer"

	err := config.Validate()

	if err != nil {
		return nil, fmt.Errorf("%s: error when validate config: %w", op, err)
	}

	saramaConfig := sarama.NewConfig()

	saramaConfig.Version = sarama.V4_1_0_0

	// Самый надежный режим для Outbox: ждем подтверждения от всех реплик (ISR).
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll

	// Ретраи
	saramaConfig.Producer.Retry.Max = config.SendMaxRetries
	saramaConfig.Producer.Retry.Backoff = config.RetryBackoff

	// Настройки сброса батча в сеть
	saramaConfig.Producer.Flush.Bytes = config.BatchBytes
	saramaConfig.Producer.Flush.Messages = config.BatchMessagesCount
	saramaConfig.Producer.Flush.Frequency = config.BatchFrequency

	saramaConfig.ChannelBufferSize = config.ChannelBufferSize

	// Обязательно для HandleSuccesses и HandleErrors в Outbox
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.Return.Successes = true

	// Snappy дает отличный баланс CPU/Compression для JSON событий.
	saramaConfig.Producer.Compression = sarama.CompressionSnappy

	p, err := sarama.NewAsyncProducer(config.Brokers, saramaConfig)

	if err != nil {
		return nil, fmt.Errorf("%s: error when create async producer instance: %w", op, err)
	}

	return &Producer{p}, nil

}

func (p *Producer) Send(ctx context.Context, event kafka.Event) error {

	const op = "core.messaging.kafka.sarama.Producer.Send"

	msg := &sarama.ProducerMessage{
		Topic: event.GetTopic(),
		Key:   sarama.StringEncoder(event.GetAggregateID()),
		Value: sarama.ByteEncoder(event.GetPayload()),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte(kafka.HeaderEventID),
				Value: []byte(event.GetID()),
			},
			{
				Key:   []byte(kafka.HeaderEventType),
				Value: []byte(event.GetType()),
			},
		},
		Metadata: kafka.MessageMetadata{
			ID:   event.GetID(),
			Type: event.GetType(),
		},
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("%s: cannot send event with id: %s, context is done: %w", op, event.GetID(), ctx.Err())
	case p.Input() <- msg:
		return nil
	}
}

func (p *Producer) HandleErrors(timeout time.Duration, onError kafka.OnError) {

	const op = "core.messaging.kafka.sarama.Producer.HandleErrors"

	for event := range p.Errors() {
		metadata, ok := event.Msg.Metadata.(kafka.MessageMetadata)

		if !ok {
			//TODO: log maybe that metadata is not provided?
			continue
		}

		failedEvent := kafka.NewFailedEvent(metadata.ID, metadata.Type, event.Err)

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err := onError(ctx, failedEvent)
		cancel()
		if err != nil {
			continue
			//TODO: log if onError callback return error
		}
	}
}

func (p *Producer) HandleSuccesses(timeout time.Duration, onSuccess kafka.OnSuccess) {

	const op = "core.messaging.kafka.sarama.Producer.HandleSuccesses"

	for event := range p.Successes() {
		metadata, ok := event.Metadata.(kafka.MessageMetadata)

		if !ok {
			//TODO: log maybe that metadata is not provided?
			continue
		}

		successEvent := kafka.NewSuccessEvent(metadata.ID, metadata.Type)

		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		err := onSuccess(ctx, successEvent)
		cancel()
		if err != nil {
			//TODO: log if onError callback return error
			continue
		}
		//TODO: log, event is confirmed successfully
	}

}

func (p *Producer) Stop(ctx context.Context) error {

	const op = "core.messaging.kafka.sarama.Producer.Stop"

	done := make(chan error, 1)

	go func() {
		err := p.Close()
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("%s: error when closing producer: %w", op, err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: %w", op, ctx.Err())
	}
}
