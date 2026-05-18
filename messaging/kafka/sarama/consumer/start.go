package consumer

import (
	"context"
	"errors"
	"sync"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

func (c *group) Start(readParams kafka.ConsumerGroupStartReadParams, onConsumeError kafka.OnConsumeError) {

	warmupDone := make(chan struct{})

	wg := &sync.WaitGroup{}

	wg.Go(func() {
		c.log.Info("warmup started in parallel...", logger.Any("topics", c.config.Topics))

		warmupCtx, cancel := context.WithTimeout(c.stopCtx, c.config.WarmupTimeout)
		defer cancel()

		if err := waitTopicsReady(warmupCtx, c.config.Brokers, c.config.Topics); err != nil {
			if errors.Is(err, context.Canceled) {
				c.log.Info("startup canceled during warmup")
			} else {
				c.log.Error("warmup failed, signaling stop", logger.Err(err))
			}

			// Сворачиваем всю группу (остальные горутины поймают <-c.stopCtx.Done())
			c.stopCtxFunc()
			return
		}

		// Всё ок, открываем барьер
		close(warmupDone)
		c.log.Info("warmup success, barrier opened")
	})

	wg.Go(func() {

		select {
		case <-warmupDone:
			c.startRead(c.stopCtx, readParams)
		case <-c.stopCtx.Done():
			c.log.Info("handler worker exiting: stopped before warmup completed")
			return
		}
	})

	wg.Go(func() {
		c.handleErrors(c.stopCtx, onConsumeError)
	})

	go func() {
		wg.Wait()
		close(c.isStopped)
	}()
}
