package main

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type EventRepository struct{}

func (r *EventRepository) UnReserve(ctx context.Context, id uuid.UUID) error {
	return nil
}

func (r *EventRepository) MarkAsDone(ctx context.Context, id uuid.UUID) error {
	return nil
}

func (r *EventRepository) Reserve(ctx context.Context, limit int, duration time.Duration) ([]Event, error) {
	return nil, nil
}

func (r *EventRepository) Create(ctx context.Context, e Event) error {
	return nil
}
