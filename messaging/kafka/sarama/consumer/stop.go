package consumer

import (
	"context"
	"fmt"
)

func (c *group) Stop(ctx context.Context) error {
	const op = "core.messaging.kafka.sarama.ConsumerGroup.Stop"
	var closeErr error

	// 1. Используем sync.Once, чтобы не закрывать дважды
	c.stopOnce.Do(func() {
		// А) Отменяем контекст (сигнал обработчикам остановиться)
		c.stopCtxFunc()

		// Б) СРАЗУ закрываем саму группу Sarama.
		// Это разорвет сетевые соединения и вытолкнет c.Consume() из блокировки.
		closeErr = c.g.Close()
	})

	// 2. Ждем, пока горутины старта реально завершатся
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
