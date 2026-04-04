package kafka

import (
	"encoding/json"
)

type FailedEvent struct {
	id    string
	etype string
	err   error
}

func NewFailedEvent(id, etype string, err error) FailedEvent {
	return FailedEvent{id: id, etype: etype, err: err}
}

func (e FailedEvent) GetID() string {
	return e.id
}

func (e FailedEvent) GetType() string {
	return e.etype
}

func (e FailedEvent) GetError() error {
	return e.err
}

type SuccessEvent struct {
	id    string
	etype string
}

func NewSuccessEvent(id, etype string) SuccessEvent {
	return SuccessEvent{id: id, etype: etype}
}

func (e SuccessEvent) GetID() string {
	return e.id
}

func (e SuccessEvent) GetType() string {
	return e.etype
}

type Event struct {
	id          string
	aggregateID string
	topic       string
	etype       string
	payload     json.RawMessage
}

func NewEvent(id, aggid, topic, etype string, payload json.RawMessage) Event {
	return Event{
		id:          id,
		aggregateID: aggid,
		topic:       topic,
		etype:       etype,
		payload:     payload,
	}
}

func (e Event) GetID() string {
	return e.id
}

func (e Event) GetType() string {
	return e.etype
}

func (e Event) GetTopic() string {
	return e.topic
}

func (e Event) GetAggregateID() string {
	return e.aggregateID
}

func (e Event) GetPayload() json.RawMessage {
	return e.payload
}

type ConsumeEvent struct {
	id        string
	etype     string
	payload   json.RawMessage
	key       []byte
	offset    int64
	topic     string
	partition int32
}

func NewConsumeEvent(
	id string,
	etype string,
	payload json.RawMessage,
	key []byte,
	offset int64,
	topic string,
	partition int32,
) ConsumeEvent {
	return ConsumeEvent{
		id:        id,
		etype:     etype,
		payload:   payload,
		key:       key,
		offset:    offset,
		topic:     topic,
		partition: partition,
	}
}

//TODO:
// type Session struct {
// }

// type S interface {
// 	Claims() map[string][]int32
// 	MemberID() string
// 	GenerationID() int32
// 	MarkOffset(topic string, partition int32, offset int64, metadata string)
// 	Commit()
// 	ResetOffset(topic string, partition int32, offset int64, metadata string)
// 	MarkMessage(msg *ConsumerMessage, metadata string)
// 	Context() context.Context
// }
