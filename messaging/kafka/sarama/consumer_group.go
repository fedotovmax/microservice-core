package sarama

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

type ConsumerGroup struct {
	sarama.ConsumerGroup
	isStopped   chan struct{}
	stopCtx     context.Context
	stopCtxFunc func()
	config      ConsumerGroupConfig
}

const maxJitter = time.Millisecond * 500

// - ReadTimeout → сколько ждём ответ (важно для commit)
// - WriteTimeout → сколько отправляем запрос
// - Session.Timeout → когда тебя выкинут из группы
// - Heartbeat.Interval → как часто ты “пингуешь” Kafka

// TODO: check settings
// config.Net.ReadTimeout = 5 * time.Second
// config.Net.WriteTimeout = 5 * time.Second
// config.Consumer.Group.Session.Timeout = 10 * time.Second
// config.Consumer.Group.Heartbeat.Interval = 3 * time.Second
func NewConsumerGroup(config ConsumerGroupConfig) (kafka.ConsumerGroup, error) {

	const op = "core.messaging.kafka.sarama.NewConsumerGroup"

	err := config.Validate()

	if err != nil {
		return nil, fmt.Errorf("%s: error when validate config: %w", op, err)
	}

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

	c, err := sarama.NewConsumerGroup(config.Brokers, config.GroupID, cfg)

	if err != nil {
		return nil, fmt.Errorf("%s: error when create consumer group instance: %w", op, err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ConsumerGroup{
		ConsumerGroup: c,
		isStopped:     make(chan struct{}),
		stopCtx:       ctx,
		stopCtxFunc:   cancel,
		config:        config,
	}, nil

}

func (c *ConsumerGroup) startRead(ctx context.Context, reader kafka.Reader) {

	currentSleep := c.config.BackoffMinInterval

	for {

		if ctx.Err() != nil {
			//TODO: log
			return
		}

		err := c.Consume(ctx, c.config.Topics, &h{r: reader, commitInterval: c.config.CommitInterval /**commitBatchSize: c.config.CommitBatchSize**/})

		if err != nil {

			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}

			//TODO: log

			jitter := time.Duration(rand.Int63n(int64(maxJitter)))
			sleepTime := currentSleep + jitter

			//TODO: log

			select {
			case <-time.After(sleepTime):
				// Увеличиваем паузу (exponential)
				currentSleep *= 2
				if currentSleep > c.config.BackoffMaxInterval {
					currentSleep = c.config.BackoffMaxInterval
				}
			case <-ctx.Done():
				return
			}

			continue

		}

		currentSleep = c.config.BackoffMinInterval

	}

}

func (c *ConsumerGroup) handleErrors(ctx context.Context, onError kafka.OnConsumeError) {

	for {

		select {
		case <-ctx.Done():

			return

		case err, ok := <-c.Errors():

			if !ok {
				//TODO: log
				return
			}

			if err != nil {
				onError(ctx, err)
			}

		}
	}

}

func (c *ConsumerGroup) Start(onError kafka.OnConsumeError, reader kafka.Reader) {

	wg := &sync.WaitGroup{}

	wg.Go(func() {
		c.startRead(c.stopCtx, reader)
	})

	wg.Go(func() {
		c.handleErrors(c.stopCtx, onError)
	})

	go func() {
		wg.Wait()
		c.signalAllStopped()
	}()

}

func (c *ConsumerGroup) signalAllStopped() {
	close(c.isStopped)
}

func (c *ConsumerGroup) Stop(ctx context.Context) error {

	const op = "core.messaging.kafka.sarama.ConsumerGroup.Stop"

	done := make(chan error, 1)

	c.stopCtxFunc()

	go func() {
		<-c.isStopped
		err := c.ConsumerGroup.Close()
		done <- err
	}()

	select {

	case err := <-done:

		if err != nil {
			return fmt.Errorf("%s: unexpected error when close consumer group: %w", op, err)
		}

		return nil

	case <-ctx.Done():

		return fmt.Errorf("%s: %w", op, ctx.Err())

	}
}
