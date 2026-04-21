package outbox

import (
	"context"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
)

func (p *Outbox) process() {

	const op = "core.messaging.kafka.outbox.Outbox.process"

	log := p.log.With(logger.String("op", op))

	defer close(p.processingFinished)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ticker := time.NewTicker(p.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stopProcessSignal:
			log.Info("event processing stopped")
			return
		case <-ticker.C:
			if !p.inProcess.CompareAndSwap(false, true) {
				continue
			}
			p.handle(ctx)
			p.inProcess.Store(false)
		}
	}

}
