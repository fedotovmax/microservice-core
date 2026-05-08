package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fedotovmax/microservice-core/ft"
	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/messaging/kafka/segmentio"
	skafka "github.com/segmentio/kafka-go"
)

type offsetTrackerKey struct {
	topic     string
	partition int
}

// offsetTracker накапливает офсеты и сбрасывает их батчем — аналог MarkMessage в Sarama
type offsetTracker struct {
	mu      sync.Mutex
	offsets map[offsetTrackerKey]skafka.Message // partition -> последнее помеченное сообщение
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{
		offsets: make(map[offsetTrackerKey]skafka.Message),
	}
}

// mark помечает сообщение как обработанное (аналог MarkMessage)
func (t *offsetTracker) mark(msg skafka.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()
	key := struct {
		topic     string
		partition int
	}{msg.Topic, msg.Partition}
	if cur, ok := t.offsets[key]; !ok || msg.Offset > cur.Offset {
		t.offsets[key] = msg
	}
}

// flush сбрасывает все помеченные офсеты и возвращает их
func (t *offsetTracker) flush() []skafka.Message {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.offsets) == 0 {
		return nil
	}
	msgs := make([]skafka.Message, 0, len(t.offsets))
	for _, msg := range t.offsets {
		msgs = append(msgs, msg)
	}
	t.offsets = make(map[offsetTrackerKey]skafka.Message)
	return msgs
}

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

	tracker := newOffsetTracker()

	// Фоновый коммит по интервалу — как autocommit в Sarama
	go func() {
		ticker := time.NewTicker(c.config.CommitInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				msgs := tracker.flush()
				if len(msgs) == 0 {
					continue
				}
				if err := c.reader.CommitMessages(ctx, msgs...); err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					select {
					case c.errCh <- fmt.Errorf("%s: auto-commit error: %w", op, err):
					case <-ctx.Done():
						return
					}
				}
			case <-ctx.Done():
				// Финальный коммит при завершении
				msgs := tracker.flush()
				if len(msgs) > 0 {
					_ = c.reader.CommitMessages(context.Background(), msgs...)
				}
				return
			}
		}
	}()

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
				tracker.mark(msg)
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
		tracker.mark(msg)
	}
}

func (c *group) handleWithTimeout(ctx context.Context, h kafka.MessageHandler, ev kafka.ConsumeMessage) error {
	readCtx, cancel := context.WithTimeout(ctx, c.config.MaxProcessingTime)
	defer cancel()
	fmt.Printf("CONSUMER HEADERS: %+v\n", ev.Headers())
	return h(readCtx, ev)
}
