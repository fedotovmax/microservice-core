package producer

import (
	"context"
	"fmt"
)

func (p *producer) Stop(ctx context.Context) error {
	const op = "core.messaging.kafka.segmentio.producer.Stop"

	done := make(chan error, 1)
	go func() {
		err := p.w.Close()
		close(p.successCh)
		close(p.errCh)
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: %w", op, ctx.Err())
	}
}
