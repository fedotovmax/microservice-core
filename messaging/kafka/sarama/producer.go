package sarama

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

type Producer struct {
	sarama.AsyncProducer
}

func (p *Producer) Send(ctx context.Context, event kafka.Event) error {

	const op = "messaging.kafka.sarama.Producer.Send"

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

func (p *Producer) HandleErrors(ctx context.Context, onError kafka.OnError) {

	const op = "messaging.kafka.sarama.Producer.HandleErrors"

	for {
		select {
		case <-ctx.Done():
			//TODO: log
			return
		case event, ok := <-p.Errors():
			if !ok {
				//TODO: log
				return
			}
			//TODO: log what event send is failed?

			metadata, ok := event.Msg.Metadata.(kafka.MessageMetadata)

			if !ok {
				//TODO: log maybe that metadata is not provided?
				continue
			}

			failedEvent := kafka.NewFailedEvent(metadata.ID, metadata.Type, event.Err)

			err := onError(ctx, failedEvent)

			if err != nil {
				//TODO: log if onError callback return error
			}
		}
	}
}

func (p *Producer) HandleSuccesses(ctx context.Context, onSuccess kafka.OnSuccess) {

	const op = "messaging.kafka.sarama.Producer.HandleSuccesses"

	for {
		select {
		case <-ctx.Done():
			//TODO: log
			return
		case event, ok := <-p.Successes():
			if !ok {
				//TODO: log
				return
			}
			//TODO: log what event send is failed?

			metadata, ok := event.Metadata.(kafka.MessageMetadata)

			if !ok {
				//TODO: log maybe that metadata is not provided?
				continue
			}

			successEvent := kafka.NewSuccessEvent(metadata.ID, metadata.Type)

			err := onSuccess(ctx, successEvent)

			if err != nil {
				//TODO: log if onError callback return error
				continue
			}
			//TODO: log, event is confirmed successfully
		}
	}
}

func (p *Producer) Stop(ctx context.Context) error {

	const op = "messaging.kafka.sarama.Producer.Stop"

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

func NewProducer(config ProducerConfig) (kafka.AsyncProducer, error) {
	const op = "messaging.kafka.sarama.NewProducer"

	err := config.Validate()

	if err != nil {
		return nil, fmt.Errorf("%s: error when validate config: %w", op, err)
	}

	saramaConfig := sarama.NewConfig()

	saramaConfig.Version = sarama.V4_1_0_0

	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll

	saramaConfig.Producer.Retry.Max = config.SendMaxRetries

	saramaConfig.Producer.Retry.Backoff = config.RetryBackoff

	saramaConfig.Producer.Flush.Bytes = config.BatchBytes

	saramaConfig.Producer.Flush.Messages = config.BatchMessagesCount

	saramaConfig.Producer.Flush.Frequency = config.BatchFrequency

	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.Return.Successes = true

	saramaConfig.Producer.Compression = sarama.CompressionSnappy

	saramaConfig.ChannelBufferSize = config.ChannelBufferSize

	p, err := sarama.NewAsyncProducer(config.Brokers, saramaConfig)

	if err != nil {
		return nil, fmt.Errorf("%s: error when create async producer instance: %w", op, err)
	}

	return &Producer{p}, nil

}
