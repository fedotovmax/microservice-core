package kafka

import (
	"context"
)

type OnSuccess func(ctx context.Context, event SuccessEvent) error
type OnError func(ctx context.Context, event FailedEvent) error

type AsyncProducer interface {
	Send(ctx context.Context, event Event) error
	HandleErrors(ctx context.Context, onError OnError)
	HandleSuccesses(ctx context.Context, onSuccess OnSuccess)
	Stop(ctx context.Context) error
}

type ConsumerGroup interface {
	Start(onError OnConsumeError, reader Reader)
	Stop(ctx context.Context) error
}

type Mark func(meta string)

type OnConsumeError func(ctx context.Context, err error)

type Reader interface {
	OnRead(ctx context.Context, event ConsumeEvent, mark Mark)
	OnSetup()
	OnCleanUp()
}
