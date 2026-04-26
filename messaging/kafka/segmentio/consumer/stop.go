package consumer

import (
	"context"
	"fmt"
)

func (c *group) Stop(ctx context.Context) error {
	const op = "core.messaging.kafka.segmentio.ConsumerGroup.Stop"
	var closeErr error

	c.stopOnce.Do(func() {
		// 1. Сигналим циклу Start остановиться
		c.stopCtxFunc()

		// 2. Закрываем ридер (прерывает сетевые ожидания)
		closeErr = c.reader.Close()
	})

	select {
	case <-c.isStopped:
		if closeErr != nil {
			return fmt.Errorf("%s: close error: %w", op, closeErr)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: shutdown timed out: %w", op, ctx.Err())
	}
}
