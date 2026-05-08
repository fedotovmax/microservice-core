package kafka

import (
	"context"
	"time"
)

type OnSuccess func(ctx context.Context, event Message) error
type OnError func(ctx context.Context, event FailedMessage) error

type AsyncProducer interface {
	Send(ctx context.Context, event Message) error
	HandleErrors(timeout time.Duration, onError OnError)
	HandleSuccesses(timeout time.Duration, onSuccess OnSuccess)
	Stop(ctx context.Context) error
}

type ConsumerGroup interface {
	Start(params ConsumerGroupStartReadParams, onConsumeError OnConsumeError)
	Stop(ctx context.Context) error
}

type MessageHandler func(ctx context.Context, ev ConsumeMessage) error

type Middleware func(next MessageHandler) MessageHandler

type OnConsumeError func(ctx context.Context, err error)

type OnSetup func() error

type OnCleanUp func() error
