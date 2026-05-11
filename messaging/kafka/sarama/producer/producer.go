package producer

import (
	"fmt"

	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type producer struct {
	ap      sarama.AsyncProducer
	log     logger.Logger
	tracer  trace.Tracer
	metrics *kafka.ProducerMetrics
}

func New(log logger.Logger, config kafka.ProducerConfig) (kafka.AsyncProducer, error) {
	const op = "core.messaging.kafka.sarama.producer.New"

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

	ap, err := sarama.NewAsyncProducer(config.Brokers, saramaConfig)

	if err != nil {
		return nil, fmt.Errorf("%s: error when create async producer instance: %w", op, err)
	}

	p := &producer{ap: ap, log: log}

	if config.Telemetry {
		p.tracer = otel.Tracer(kafka.TracerName)
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

func (p *producer) withTracer() bool {
	return p.tracer != nil
}
