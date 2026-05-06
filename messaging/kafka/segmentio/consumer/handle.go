package consumer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/fedotovmax/microservice-core/ft"
	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/messaging/kafka/segmentio"
)

func (c *group) handle(ctx context.Context, onSetup kafka.OnSetup, onCleanUp kafka.OnCleanUp, h kafka.MessageHandler) {

	const op = "core.messaging.kafka.segmentio.consumer.group.handle"

	log := c.log.With(logger.String("op", op))

	defer close(c.errCh)

	// Setup
	if onSetup != nil {
		if err := onSetup(); err != nil {
			// Используем select с контекстом, чтобы не зависнуть при закрытии
			select {
			case c.errCh <- fmt.Errorf("%s: setup failed: %w", op, err):
			case <-ctx.Done():
			}
			return
		}
	}

	if onCleanUp != nil {
		defer onCleanUp()
	}

	bo := ft.NewExponentialBackoff(c.config.BackoffMinInterval, c.config.BackoffMaxInterval, 0.1)
	attempt := 0

	for {
		if ctx.Err() != nil {
			return
		}

		// Блокирующее чтение из Kafka
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}

			// Отправляем ошибку инфраструктуры в воркер
			select {
			case c.errCh <- fmt.Errorf("%s: fetch error: %w", op, err):
			case <-ctx.Done():
				return
			}

			// Ожидание перед следующим ретраем (Backoff)
			attempt++
			wait := bo.Next(attempt)
			timer := time.NewTimer(wait)

			select {
			case <-timer.C:
				// Таймер истек, пробуем снова
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
			continue
		}

		// Успешный Fetch — сбрасываем счетчик ретраев
		attempt = 0

		ev := kafka.NewConsumeMessage(
			msg.Value,
			msg.Key,
			msg.Offset,
			msg.Topic,
			int32(msg.Partition),
			segmentio.HeadersFromSegmentio(msg.Headers),
		)

		// Обработка бизнес-логики (синхронно для сохранения порядка в партиции)
		if err := c.handleWithTimeout(ctx, h, ev); err != nil {
			if _, ok := errors.AsType[*kafka.NoRetryError](err); ok {
				// Пропускаем сообщение (коммитим офсет)
				_ = c.reader.CommitMessages(ctx, msg)
				continue
			}

			// Ошибки хендлера просто логируем, как в Sarama
			log.Error("an unexpected error was received while processing the message",
				logger.Err(err),
				logger.String("topic", msg.Topic),
				logger.Int64("offset", msg.Offset),
			)
			continue
		}

		// Коммит офсета после успешной обработки
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			select {
			case c.errCh <- fmt.Errorf("%s: commit error: %w", op, err):
			case <-ctx.Done():
				return
			}
		}
	}
}

func (c *group) handleWithTimeout(ctx context.Context, h kafka.MessageHandler, ev kafka.ConsumeMessage) error {
	readCtx, cancel := context.WithTimeout(ctx, c.config.MaxProcessingTime)
	defer cancel()
	return h(readCtx, ev)
}
