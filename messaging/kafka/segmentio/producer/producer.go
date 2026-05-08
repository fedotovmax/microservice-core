package producer

import (
	"context"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	skafka "github.com/segmentio/kafka-go"
)

type segmentioWriter interface {
	WriteMessages(ctx context.Context, msgs ...skafka.Message) error
	Close() error
}

type errMessage struct {
	skafka.Message
	Error error
}

type producer struct {
	w         segmentioWriter
	log       logger.Logger
	successCh chan skafka.Message
	errCh     chan errMessage // В segmentio ошибка лежит внутри Message при использовании Completion
}

func New(log logger.Logger, config kafka.ProducerConfig) (kafka.AsyncProducer, error) {
	const op = "core.messaging.kafka.segmentio.producer.New"

	// Каналы для связи Completion callback с методами Handle
	successCh := make(chan skafka.Message, config.ChannelBufferSize)
	errCh := make(chan errMessage, config.ChannelBufferSize)

	w := &skafka.Writer{
		Addr:            skafka.TCP(config.Brokers...),
		Balancer:        &skafka.Hash{},
		RequiredAcks:    skafka.RequiredAcks(int(skafka.RequireAll)), // WaitForAll
		MaxAttempts:     config.SendMaxRetries,
		WriteBackoffMin: config.RetryBackoff,
		BatchSize:       config.BatchMessagesCount,
		BatchBytes:      int64(config.BatchBytes),
		BatchTimeout:    config.BatchFrequency,
		Compression:     skafka.Snappy,
		Async:           true, // Позволяет Send не блокироваться
		// Completion срабатывает на каждое сообщение после попытки записи
		Completion: func(messages []skafka.Message, err error) {
			for _, m := range messages {

				if err != nil {
					// В segmentio Completion получает ошибку на весь батч.
					// Мы прокидываем сообщение дальше, сохранив ошибку в замыкании
					// или обернув сообщение.
					// Но проще всего передать её через кастомный механизм.
					em := errMessage{
						Message: m,
						Error:   err,
					}
					errCh <- em
				} else {
					successCh <- m
				}
			}
		},
	}

	var writer segmentioWriter = w

	if config.Tracing {
		writer = newTracedWriter(w)
	}

	return &producer{
		w:         writer,
		log:       log,
		successCh: successCh,
		errCh:     errCh,
	}, nil
}
