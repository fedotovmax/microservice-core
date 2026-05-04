package redis

import (
	"context"
	"fmt"
	"strings"

	"github.com/fedotovmax/microservice-core/conc"
	"github.com/fedotovmax/microservice-core/ft"
	"github.com/fedotovmax/microservice-core/logger"
	"github.com/redis/go-redis/v9"
)

type Handler func(context.Context, Message) error

func (p *client) Publish(ctx context.Context, channel string, value any) error {
	err := p.rc.Publish(ctx, channel, value).Err()
	if err != nil {
		return fmt.Errorf("%s: %w", opPublish, err)
	}
	return nil
}

func (p *client) Subscribe(exCtx context.Context, handler Handler, channels ...string) {

	subCtx, cancelSub := context.WithCancel(exCtx)

	stopObserver := context.AfterFunc(p.subsCtx, func() {
		cancelSub()
	})

	p.subsWg.Go(func() {

		defer stopObserver()
		defer cancelSub()

		backoff := ft.NewExponentialBackoff(
			p.cfg.PubSubRetryWaitFrom,
			p.cfg.MaxRetryBackoff,
			0.2,
		)

		err := ft.Retry(subCtx, backoff, p.cfg.MaxRetries, ft.RetryAlwaysPolicy, func() error {
			if err := p.subscribe(subCtx, handler, channels...); err != nil {
				if p.log != nil {
					p.log.Error(fmt.Sprintf("%s: subscription lost, reconnecting...", opSubscribe))
				}
				return err
			}
			return nil
		})

		if err != nil {
			if p.log != nil {
				p.log.With(
					logger.String("op", opSubscribe),
				).
					Error(
						"subscription stopped permanently",
						logger.String("channels", strings.Join(channels, ",")),
						logger.Err(err),
					)
			}
		}
	})
}

func (p *client) subscribe(ctx context.Context, handler Handler, channels ...string) error {
	sub := p.rc.Subscribe(ctx, channels...)
	defer sub.Close()

	// Ждем подтверждения успешной подписки
	if _, err := sub.Receive(ctx); err != nil {
		return fmt.Errorf("%s: %w", op_subscribe, err)
	}

	// Передаем канал от go-redis напрямую в воркерпул
	results := conc.Workerpool(ctx, sub.Channel(), workerPoolSize, func(ctx context.Context, msg *redis.Message) (struct{}, error) {
		return struct{}{}, handler(
			ctx,
			Message{
				Channel:      msg.Channel,
				Pattern:      msg.Pattern,
				Payload:      msg.Payload,
				PayloadSlice: msg.PayloadSlice,
			})
	})

	// Читаем результаты работы воркеров
	for res := range results {
		if res.Err != nil {
			if p.log != nil {
				p.log.With(
					logger.String("op", op_subscribe),
				).
					Error("error when handle message", logger.Err(res.Err))
			}
		}
	}

	// ВАЖНО: Мы вышли из цикла. Это значит, что канал от Redis закрылся.
	// Если контекст был отменен (штатное завершение микросервиса), возвращаем ctx.Err().
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%s: %w", op_subscribe, err)
	}

	// В противном случае это обрыв соединения со стороны Redis.
	// Мы ОБЯЗАНЫ вернуть ошибку, чтобы сработал механизм Retry!
	return fmt.Errorf("%s: redis channel closed unexpectedly", op_subscribe)
}
