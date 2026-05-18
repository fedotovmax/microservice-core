package consumer

import (
	"context"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

type group struct {
	log         logger.Logger
	g           sarama.ConsumerGroup
	isStopped   chan struct{}
	stopCtx     context.Context
	stopCtxFunc func()
	config      kafka.GroupConfig
	stopOnce    sync.Once // Гарантирует, что закроем всё один раз
}

var (
	cg      kafka.ConsumerGroup
	once    sync.Once
	initErr error
)

func NewGroup(log logger.Logger, config kafka.GroupConfig) (kafka.ConsumerGroup, error) {

	const op = "core.messaging.kafka.sarama.consumer.NewGroup"

	once.Do(func() {
		cfg := sarama.NewConfig()

		cfg.Version = sarama.V4_1_0_0

		cfg.Consumer.Return.Errors = true
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
		cfg.Consumer.Offsets.AutoCommit.Enable = true
		cfg.Consumer.Offsets.AutoCommit.Interval = config.CommitInterval

		cfg.Net.DialTimeout = config.DialTimeout
		cfg.Net.ReadTimeout = config.ReadTimeout

		cfg.Consumer.Group.Session.Timeout = config.SessionTimeout
		cfg.Consumer.Group.Heartbeat.Interval = config.HeartbeatInterval

		cfg.Consumer.Group.Rebalance.Timeout = config.RebalanceTimeout

		cfg.Consumer.MaxWaitTime = config.MaxWaitTime

		cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}
		cfg.Consumer.MaxProcessingTime = config.MaxProcessingTime

		g, err := sarama.NewConsumerGroup(config.Brokers, config.GroupID, cfg)

		if err != nil {
			initErr = fmt.Errorf("%s: error when create consumer group instance: %w", op, err)
			return
		}

		ctx, cancel := context.WithCancel(context.Background())

		cg = &group{
			g:           g,
			isStopped:   make(chan struct{}),
			stopCtx:     ctx,
			stopCtxFunc: cancel,
			config:      config,
			log:         log,
		}
	})

	if initErr != nil {
		return nil, initErr
	}

	return cg, nil
}
