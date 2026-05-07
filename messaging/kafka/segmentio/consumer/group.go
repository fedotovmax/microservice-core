package consumer

import (
	"context"
	"fmt"
	"sync"

	otelkafka "github.com/Trendyol/otel-kafka-konsumer"
	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	skafka "github.com/segmentio/kafka-go"
)

type segmentioReader interface {
	FetchMessage(ctx context.Context) (skafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...skafka.Message) error
	Close() error
}

type group struct {
	log         logger.Logger
	reader      segmentioReader
	isStopped   chan struct{}
	errCh       chan error
	stopCtx     context.Context
	stopCtxFunc func()
	config      kafka.GroupConfig
	stopOnce    sync.Once
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
		CommitInterval:   config.CommitInterval,

		StartOffset: skafka.FirstOffset,
	})

	var reader segmentioReader

	if config.Tracing {
		otelReader, err := otelkafka.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		reader = newOtel(otelReader)
	} else {
		reader = r
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &group{
		log:         log,
		reader:      reader,
		isStopped:   make(chan struct{}),
		errCh:       make(chan error, 128),
		stopCtx:     ctx,
		stopCtxFunc: cancel,
		config:      config,
	}, nil
}
