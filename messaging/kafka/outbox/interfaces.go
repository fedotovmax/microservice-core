package outbox

import (
	"context"
	"time"
)

type Adapter interface {
	MarkAsFailed(ctx context.Context, event FailedEvent) error
	Confirm(ctx context.Context, event SuccessEvent) error
	Reserve(ctx context.Context, limit int, duration time.Duration) ([]Event, error)
}

type Creator interface {
	Create(ctx context.Context, in Input) (string, error)
}
