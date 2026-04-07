package sarama

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

type h struct {
	r                 kafka.Reader
	commitInterval    time.Duration
	maxProcessingTime time.Duration
}

func (h *h) Setup(s sarama.ConsumerGroupSession) error {
	//TODO: adapter s to OnSetup (later)
	h.r.OnSetup()
	return nil
}

func (h *h) Cleanup(s sarama.ConsumerGroupSession) error {
	//TODO: adapter s to OnCleanUp (later)
	h.r.OnCleanUp()

	return nil
}

func (h *h) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {

	for {
		select {
		case <-s.Context().Done():
			return nil

		case msg, ok := <-c.Messages():
			if !ok {
				return nil
			}

			mark := func(meta string) {
				s.MarkMessage(msg, meta)
			}

			var eventID string
			var eventType string

			for _, header := range msg.Headers {
				key := string(header.Key)
				switch key {
				case kafka.HeaderEventType:
					eventType = string(header.Value)
				case kafka.HeaderEventID:
					eventID = string(header.Value)
				}
			}

			if eventID == "" || eventType == "" {
				// Пропускаем битые данные, чтобы не копить очередь
				mark(fmt.Sprintf("empty headers: EventID: %s; EventType: %s", eventID, eventType))
				continue
			}

			payload := msg.Value

			ev := kafka.NewConsumeEvent(eventID, eventType, payload, msg.Key, msg.Offset, msg.Topic, msg.Partition)

			ctx, cancel := context.WithTimeout(s.Context(), h.maxProcessingTime)
			h.r.OnRead(ctx, ev, mark)
			cancel()
		}
	}
}
