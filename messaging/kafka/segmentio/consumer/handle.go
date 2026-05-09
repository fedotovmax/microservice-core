package consumer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/fedotovmax/microservice-core/ft"
	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	"github.com/fedotovmax/microservice-core/messaging/kafka/segmentio"
	"github.com/fedotovmax/microservice-core/observability"
	skafka "github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
	"go.opentelemetry.io/otel/trace"
)

type offsetTrackerKey struct {
	topic     string
	partition int
}

// offsetTracker накапливает офсеты и сбрасывает их батчем — аналог MarkMessage в Sarama
type offsetTracker struct {
	mu      sync.Mutex
	offsets map[offsetTrackerKey]skafka.Message // partition -> последнее помеченное сообщение
	onMark  func(msg skafka.Message)            // колбэк на марк

}

func newOffsetTracker(onMark func(msg skafka.Message)) *offsetTracker {
	return &offsetTracker{
		offsets: make(map[offsetTrackerKey]skafka.Message),
		onMark:  onMark,
	}
}

// mark помечает сообщение как обработанное (аналог MarkMessage)
func (t *offsetTracker) mark(msg skafka.Message) {
	t.mu.Lock()
	var notify bool
	key := offsetTrackerKey{msg.Topic, msg.Partition}
	if cur, ok := t.offsets[key]; !ok || msg.Offset > cur.Offset {
		t.offsets[key] = msg
		notify = true
	}
	t.mu.Unlock()

	if notify && t.onMark != nil {
		t.onMark(msg)
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

	tracker := newOffsetTracker(func(msg skafka.Message) {
		if c.tracer != nil {
			msgCtx := otel.GetTextMapPropagator().Extract(ctx, msgReaderCarrier(msg.Headers))
			_, span := c.tracer.Start(msgCtx, kafka.TraceConsumerHandleMark,
				trace.WithSpanKind(trace.SpanKindConsumer),
				trace.WithAttributes(
					semconv.MessagingSystemKey.String(kafka.TraceSystemKey),
					semconv.MessagingDestinationName(msg.Topic),
					semconv.MessagingMessageIDKey.String(strconv.FormatInt(msg.Offset, 10)),
					semconv.MessagingKafkaMessageKeyKey.String(string(msg.Key)),
					semconv.MessagingKafkaDestinationPartitionKey.Int64(int64(msg.Partition)),
				),
			)
			if len(msg.Headers) > 0 {
				headerAttrs := make([]attribute.KeyValue, 0, len(msg.Headers))

				for _, h := range msg.Headers {
					key := string(h.Key)
					if key == observability.TraceParent {
						continue
					}
					attrKey := kafka.TraceHeaderKey(key)
					headerAttrs = append(headerAttrs, attribute.String(attrKey, string(h.Value)))
				}
				span.SetAttributes(headerAttrs...)
			}
			span.End()
		}
	})

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
	return h(readCtx, ev)
}
