package consumer

import (
	"sync"

	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

func (c *group) Start(p kafka.ConsumerGroupStartReadParams, onConsumeError kafka.OnConsumeError) {

	h := p.MessageHandler
	for i := len(p.Middlewares) - 1; i >= 0; i-- {
		h = p.Middlewares[i](h)
	}

	wg := &sync.WaitGroup{}

	wg.Go(func() {
		c.handleErrors(c.stopCtx, onConsumeError)
	})

	wg.Go(func() {
		c.handle(c.stopCtx, p.OnSetup, p.OnCleanUp, h)
	})

	go func() {
		wg.Wait()
		close(c.isStopped)
	}()

}
