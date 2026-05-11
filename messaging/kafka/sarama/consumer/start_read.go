package consumer

import (
	"context"
	"errors"
	"time"

	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/ft"
	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

func (c *group) startRead(ctx context.Context, readParams kafka.ConsumerGroupStartReadParams) {

	const op = "core.messaging.kafka.sarama.consumer.group.startRead"

	log := c.log.With(logger.String("op", op))

	// Создаем наш backoff-объект
	bo := ft.NewExponentialBackoff(
		c.config.BackoffMinInterval,
		c.config.BackoffMaxInterval,
		0.1, // 10% jitter
	)

	attempt := 0

	for {
		if ctx.Err() != nil {
			return
		}

		gh := newGroupHandler(c.log, readParams, c.config.MaxProcessingTime, c.config.Telemetry)

		err := c.g.Consume(ctx, c.config.Topics, gh)

		if err == nil {
			// Если чтение прошло успешно (Consume завершился без ошибки),
			// сбрасываем счетчик попыток для Backoff
			attempt = 0
			continue
		}

		// Если критическая ошибка — выходим
		if errors.Is(err, sarama.ErrClosedConsumerGroup) {
			log.Error("consumer group was closed, stopping reading process")
			return
		}

		// Вычисляем время ожидания на основе количества неудач подряд
		wait := bo.Next(attempt)
		attempt++

		// Безопасное ожидание
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			// Продолжаем цикл и пробуем снова
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}
