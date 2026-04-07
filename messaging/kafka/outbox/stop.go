package outbox

import (
	"context"
	"fmt"

	"github.com/fedotovmax/microservice-core/logger"
)

func (p *Outbox) Stop(ctx context.Context) error {

	const op = "core.messaging.kafka.outbox.Outbox.Stop"

	l := p.log.With(logger.String("op", op))

	close(p.stopProcessSignal)

	err := p.producer.Stop(ctx)
	if err != nil {
		l.Error("error when close producer", logger.Err(err))
	}

	select {
	case <-p.isStopped:
		l.Info("outbox stopped gracefully")
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: %w", op, ctx.Err())
	}
}
