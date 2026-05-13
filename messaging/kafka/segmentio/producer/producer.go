package producer

import (
	"fmt"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/telemetry"
	skafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/trace"
)

type errMessage struct {
	Msg skafka.Message
	Err error
}

type producer struct {
	w         *skafka.Writer
	log       logger.Logger
	successCh chan skafka.Message
	errCh     chan errMessage // В segmentio ошибка лежит внутри Message при использовании Completion
	tracer    trace.Tracer
	metrics   *kafka.ProducerMetrics
}

func New(log logger.Logger, config kafka.ProducerConfig) (kafka.AsyncProducer, error) {
	const op = "core.messaging.kafka.segmentio.producer.New"

	// Каналы для связи Completion callback с методами Handle
	successCh := make(chan skafka.Message, config.ChannelBufferSize)
	errCh := make(chan errMessage, config.ChannelBufferSize)

	w := &skafka.Writer{
		Addr:                   skafka.TCP(config.Brokers...),
		Balancer:               &skafka.Hash{},
		RequiredAcks:           skafka.RequiredAcks(int(skafka.RequireAll)), // WaitForAll
		MaxAttempts:            config.SendMaxRetries,
		WriteBackoffMin:        config.RetryBackoff,
		BatchSize:              config.BatchMessagesCount,
		BatchBytes:             int64(config.BatchBytes),
		BatchTimeout:           config.BatchFrequency,
		Compression:            skafka.Snappy,
		AllowAutoTopicCreation: true,
		Async:                  true, // Позволяет Send не блокироваться

		// Completion срабатывает на каждое сообщение после попытки записи
		Completion: func(messages []skafka.Message, err error) {
			for _, m := range messages {

				if err != nil {
					// В segmentio Completion получает ошибку на весь батч.
					// Мы прокидываем сообщение дальше, сохранив ошибку в замыкании
					// или обернув сообщение.
					// Но проще всего передать её через кастомный механизм.
					em := errMessage{
						Msg: m,
						Err: err,
					}
					errCh <- em
				} else {
					successCh <- m
				}
			}
		},
	}

	p := &producer{
		w:         w,
		log:       log,
		successCh: successCh,
		errCh:     errCh,
	}

	if config.Telemetry {
		p.tracer = telemetry.CreatePlatformTrace(kafka.PlatformTelemetryProducer)
		metrics, err := kafka.NewProducerMetrics()
		if err != nil {
			return nil, fmt.Errorf("%s: failed to init metrics: %w", op, err)
		}
		p.metrics = metrics
	}

	return p, nil
}

func (p *producer) withMetrics() bool {
	return p.metrics != nil
}

func (p *producer) withTracing() bool {
	return p.tracer != nil
}
