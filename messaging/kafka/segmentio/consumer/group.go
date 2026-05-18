package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/telemetry"
	skafka "github.com/segmentio/kafka-go"
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
	withMetrics bool
}

var (
	cg      kafka.ConsumerGroup
	once    sync.Once
	initErr error
)

func NewGroup(log logger.Logger, config kafka.GroupConfig) (kafka.ConsumerGroup, error) {

	const op = "core.messaging.kafka.segmentio.consumer.NewGroup"

	once.Do(func() {
		r := skafka.NewReader(skafka.ReaderConfig{
			Brokers:     config.Brokers,
			GroupID:     config.GroupID,
			GroupTopics: config.Topics, // Поддержка нескольких топиков
			Dialer: &skafka.Dialer{
				Timeout: config.DialTimeout,
			},

			ReadBatchTimeout:      config.ReadTimeout,
			SessionTimeout:        config.SessionTimeout,
			RebalanceTimeout:      config.RebalanceTimeout,
			HeartbeatInterval:     config.HeartbeatInterval,
			StartOffset:           skafka.FirstOffset,
			WatchPartitionChanges: true,
		})

		stopCtx, stopCtxFunc := context.WithCancel(context.Background())

		c := &group{
			log:         log,
			reader:      r,
			isStopped:   make(chan struct{}),
			errCh:       make(chan error, 128),
			stopCtx:     stopCtx,
			stopCtxFunc: stopCtxFunc,
			config:      config,
		}

		if config.Telemetry {
			c.tracer = telemetry.CreatePlatformTrace(kafka.PlatformTraceConsumer)
			err := kafka.InitConsumerInfraMetrics()

			if err != nil {
				initErr = fmt.Errorf("%s: failed to init metrics: %w", op, err)
				return
			}
			c.withMetrics = true
		}
		cg = c
	})

	if initErr != nil {
		return nil, initErr
	}

	return cg, nil
}
