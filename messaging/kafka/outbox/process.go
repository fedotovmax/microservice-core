package outbox

import (
	"context"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
)

func (p *Outbox) process(ctx context.Context) {

	const op = "messaging.kafka.outbox.process"

	log := p.log.With(logger.String("op", op))

	ticker := time.NewTicker(p.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
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
