package outbox

import (
	"context"
	"time"
)

type AsyncPublisher interface {
	Send(ctx context.Context, event Event) error
	HandleErrors(timeout time.Duration, onError OnError)
	HandleSuccesses(timeout time.Duration, onSuccess OnSuccess)
	Stop(ctx context.Context) error
}

type OnSuccess func(ctx context.Context, event Event) error
type OnError func(ctx context.Context, event FailedEvent) error

type DataSource interface {
	MarkAsFailed(ctx context.Context, event FailedEvent) error
	Confirm(ctx context.Context, event Event) error
	Reserve(ctx context.Context, limit int, duration time.Duration) ([]Event, error)
}
