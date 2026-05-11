package consumer

import (
	"context"
	"fmt"
)

func (c *group) Stop(ctx context.Context) error {
	const op = "core.messaging.kafka.sarama.ConsumerGroup.Stop"
	var closeErr error

	c.stopOnce.Do(func() {
		c.stopCtxFunc()
		closeErr = c.g.Close()
	})

	select {
	case <-c.isStopped:
		if closeErr != nil {
			return fmt.Errorf("%s: error when closing: %w", op, closeErr)
		}
		return nil

	case <-ctx.Done():
		// Если бизнес-логика (OnRead) настолько зависла, что даже Close() не помог
		return fmt.Errorf("%s: shutdown timed out: %w", op, ctx.Err())
	}
}
