package main

import (
	"context"
	"fmt"
	"time"

	"github.com/fedotovmax/microservice-core/outbox"
	"github.com/google/uuid"
)

type EventStatus string

const (
	EventStatusNew  EventStatus = "new"
	EventStatusDone EventStatus = "done"
)

type Event struct {
	ID          uuid.UUID
	AggregateID string
	Topic       string
	Type        string
	Payload     []byte // jsonb отлично ложится на []byte в Go
	Status      EventStatus
	CreatedAt   time.Time
	ReservedTo  *time.Time // Указатель, так как поле может быть NULL
}

func NewEvent(aggid, topic, ttype string, payload []byte) Event {
	return Event{
		ID:          uuid.New(),
		AggregateID: aggid,
		Topic:       topic,
		Type:        ttype,
		Payload:     payload,
		Status:      EventStatusNew,
		CreatedAt:   time.Now().UTC(),
	}
}

type Meta struct {
	EventID   uuid.UUID
	EventType string
}

type Repository interface {
	UnReserve(ctx context.Context, id uuid.UUID) error
	MarkAsDone(ctx context.Context, id uuid.UUID) error
	Reserve(ctx context.Context, limit int, duration time.Duration) ([]Event, error)
	Create(ctx context.Context, e Event) error
}

type DataSourceAdapter struct {
	repo Repository
}

type Creator interface {
	Create(ctx context.Context, event Event) (uuid.UUID, error)
}

func (dsa *DataSourceAdapter) Confirm(ctx context.Context, event outbox.Event) error {

	meta, ok := event.InternalMeta().(Meta)

	if !ok {
		return fmt.Errorf("failed when parse metadata")
	}

	return dsa.repo.MarkAsDone(ctx, meta.EventID)
}

func (dsa *DataSourceAdapter) Reserve(ctx context.Context, limit int, duration time.Duration) ([]outbox.Event, error) {
	reserved, err := dsa.repo.Reserve(ctx, limit, duration)

	if err != nil {
		return nil, err
	}

	outboxEvents := make([]outbox.Event, 0, len(reserved))

	for _, ev := range reserved {

		meta := Meta{EventID: ev.ID, EventType: ev.Type}

		outboxEvents = append(
			outboxEvents,
			outbox.NewEvent(
				ev.AggregateID,
				ev.Topic,
				ev.Payload,
				[]outbox.Header{
					{
						Key:   []byte("event_id"),
						Value: []byte(ev.ID.String()),
					},
					{
						Key:   []byte("event_type"),
						Value: []byte(ev.Type),
					},
				},
				meta,
			),
		)
	}

	return outboxEvents, nil

}

func (dsa *DataSourceAdapter) MarkAsFailed(ctx context.Context, event outbox.FailedEvent) error {

	meta, ok := event.Event().InternalMeta().(Meta)

	if !ok {
		return fmt.Errorf("failed when parse metadata")
	}

	return dsa.repo.UnReserve(ctx, meta.EventID)

}

func (dsa *DataSourceAdapter) Create(ctx context.Context, in Event) (uuid.UUID, error) {
	err := dsa.repo.Create(ctx, in)

	if err != nil {
		return uuid.Nil, err
	}

	return in.ID, nil
}
