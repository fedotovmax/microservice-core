package consumer

import (
	"sync"

	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

func (c *group) Start(readParams kafka.ConsumerGroupStartReadParams, onConsumeError kafka.OnConsumeError) {

	wg := &sync.WaitGroup{}

	wg.Go(func() {
		c.startRead(c.stopCtx, readParams)
	})

	wg.Go(func() {
		c.handleErrors(c.stopCtx, onConsumeError)
	})

	go func() {
		wg.Wait()
		close(c.isStopped)
	}()
}
