package sarama

import (
	"encoding/json"
)

type failedEvent struct {
	ID    string
	Type  string
	Error error
}

func (fe failedEvent) GetID() string {

	return fe.ID
}

func (fe failedEvent) GetType() string {
	return fe.Type
}

func (fe failedEvent) GetError() error {
	return fe.Error
}

type successEvent struct {
	ID   string
	Type string
}

func (se successEvent) GetID() string {
	return se.ID
}

func (se successEvent) GetType() string {
	return se.Type
}

type consumeEvent struct {
	ID        string
	Type      string
	Payload   json.RawMessage
	Key       []byte
	Offset    int64
	Partition int32
	Topic     string
	MarkFn    func(meta string)
}

func (e consumeEvent) GetPartition() int32 {
	return e.Partition
}

func (e consumeEvent) GetOffset() int64 {
	return e.Offset
}

func (e consumeEvent) GetKey() []byte {
	return e.Key
}

func (e consumeEvent) GetTopic() string {
	return e.Topic
}

func (e consumeEvent) GetID() string {
	return e.ID
}

func (e consumeEvent) GetType() string {
	return e.Type
}

func (e consumeEvent) GetPayload() json.RawMessage {
	return e.Payload
}

func (e consumeEvent) Mark(meta string) {
	e.MarkFn(meta)
}
