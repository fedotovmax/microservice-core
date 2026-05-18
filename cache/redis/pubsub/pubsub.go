package pubsub

import (
	"context"
	"fmt"
	"strings"
	"sync"

	redisClient "github.com/fedotovmax/microservice-core/cache/redis"
	"github.com/fedotovmax/microservice-core/conc"
	"github.com/fedotovmax/microservice-core/ft"
	"github.com/fedotovmax/microservice-core/logger"
	"github.com/redis/go-redis/v9"
)

const workerPoolSize = 10

type PubSubOption func(*pubSub)

func WithLogger(log logger.Logger) PubSubOption {
	return func(p *pubSub) {
		p.log = log
	}
}

type pubSub struct {
	rc      redisClient.PubSubClient
	cfg     Config
	log     logger.Logger
	subsWg  sync.WaitGroup
	subsCtx context.Context
	cancel  context.CancelFunc
}

// New создаёт PubSub поверх существующего *redis.Client.
// rc — нативный клиент из вашего redis.Client.Native().
func New(rc redisClient.PubSubClient, cfg Config, opts ...PubSubOption) PubSub {
	subsCtx, cancel := context.WithCancel(context.Background())

	p := &pubSub{
		rc:      rc,
		cfg:     cfg,
		subsCtx: subsCtx,
		cancel:  cancel,
	}

	for _, opt := range opts {
		opt(p)
	}

	return p
}

func (p *pubSub) Publish(ctx context.Context, channel string, value any) error {

	const op = "core.cache.redis.pubsub.Publish"

	if err := p.rc.Publish(ctx, channel, value).Err(); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	return nil
}

func (p *pubSub) Subscribe(exCtx context.Context, handler Handler, channels ...string) {

	const op = "core.cache.redis.pubsub.Subscribe"

	subCtx, cancelSub := context.WithCancel(exCtx)

	// Останавливаем подписку когда останавливается весь pubsub
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
					p.log.Error(fmt.Sprintf("%s: subscription lost, reconnecting...", op))
				}
				return err
			}
			return nil
		})

		if err != nil && p.log != nil {
			p.log.With(
				logger.String("op", op),
			).Error(
				"subscription stopped permanently",
				logger.String("channels", strings.Join(channels, ",")),
				logger.Err(err),
			)
		}
	})
}

func (p *pubSub) subscribe(ctx context.Context, handler Handler, channels ...string) error {

	const op = "core.cache.redis.pubsub.subscribe"

	sub := p.rc.Subscribe(ctx, channels...)
	defer sub.Close()

	// Ждём подтверждения успешной подписки
	if _, err := sub.Receive(ctx); err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	results := conc.Workerpool(ctx, sub.Channel(), workerPoolSize, func(ctx context.Context, msg *redis.Message) (struct{}, error) {
		return struct{}{}, handler(ctx, Message{
			Channel:      msg.Channel,
			Pattern:      msg.Pattern,
			Payload:      msg.Payload,
			PayloadSlice: msg.PayloadSlice,
		})
	})

	for res := range results {
		if res.Err != nil && p.log != nil {
			p.log.With(
				logger.String("op", op),
			).Error("error when handle message", logger.Err(res.Err))
		}
	}

	// Канал закрылся — разбираемся почему
	if err := ctx.Err(); err != nil {
		// Штатное завершение — ctx отменён снаружи
		return fmt.Errorf("%s: %w", op, err)
	}

	// Обрыв соединения — возвращаем ошибку чтобы сработал Retry
	return fmt.Errorf("%s: redis channel closed unexpectedly", op)
}

func (p *pubSub) Stop(ctx context.Context) error {

	const op = "core.cache.redis.pubsub.Stop"

	p.cancel()

	waitCh := make(chan struct{})
	go func() {
		p.subsWg.Wait()
		close(waitCh)
	}()

	select {
	case <-waitCh:
		if p.log != nil {
			p.log.With(logger.String("op", op)).Info("All redis subscriptions stopped gracefully")
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: timeout waiting for subscriptions to stop: %w", op, ctx.Err())
	}
}
