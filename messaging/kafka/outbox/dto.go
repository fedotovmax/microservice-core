package outbox

import (
	"encoding/json"

	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

type Input struct {
	aggregateID string
	topic       string
	etype       string
	payload     json.RawMessage
}

func NewInput(
	aggregateID string,
	topic string,
	etype string,
	payload json.RawMessage,
) Input {
	return Input{
		aggregateID: aggregateID,
		topic:       topic,
		etype:       etype,
		payload:     payload,
	}
}

func (e Input) GetType() string {
	return e.etype
}

func (e Input) GetTopic() string {
	return e.topic
}

func (e Input) GetAggregateID() string {
	return e.aggregateID
}

func (e Input) GetPayload() json.RawMessage {
	return e.payload
}

type Event = kafka.Event
type FailedEvent = kafka.FailedEvent
type SuccessEvent = kafka.SuccessEvent
