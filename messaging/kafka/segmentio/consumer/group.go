package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	skafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type group struct {
	log         logger.Logger
	reader      *skafka.Reader
	isStopped   chan struct{}
	errCh       chan error
	stopCtx     context.Context
	stopCtxFunc func()
	config      kafka.GroupConfig
	stopOnce    sync.Once
	tracer      trace.Tracer
	metrics     *kafka.ConsumerInfraMetrics
}

func NewGroup(log logger.Logger, config kafka.GroupConfig) (kafka.ConsumerGroup, error) {
	const op = "core.messaging.kafka.segmentio.consumer.NewGroup"

	r := skafka.NewReader(skafka.ReaderConfig{
		Brokers:     config.Brokers,
		GroupID:     config.GroupID,
		GroupTopics: config.Topics, // Поддержка нескольких топиков
		Dialer: &skafka.Dialer{
			Timeout: config.DialTimeout,
		},

		ReadBatchTimeout: config.ReadTimeout,
		SessionTimeout:   config.SessionTimeout,
		RebalanceTimeout: config.RebalanceTimeout,

		StartOffset: skafka.FirstOffset,
	})

	ctx, cancel := context.WithCancel(context.Background())

	c := &group{
		log:         log,
		reader:      r,
		isStopped:   make(chan struct{}),
		errCh:       make(chan error, 128),
		stopCtx:     ctx,
		stopCtxFunc: cancel,
		config:      config,
	}

	if config.Telemetry {
		c.tracer = otel.Tracer(kafka.TracerName)
		metrics, err := kafka.NewConsumerInfraMetrics()

		if err != nil {
			return nil, fmt.Errorf("%s: failed to init metrics: %w", op, err)
		}
		c.metrics = metrics
	}

	return c, nil
}

func (c *group) withMetrics() bool {
	return c.metrics != nil
}
