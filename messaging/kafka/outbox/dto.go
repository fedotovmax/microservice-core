package outbox

import (
	"encoding/json"

	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

type Input struct {
	key     string
	topic   string
	headers []Header
	payload json.RawMessage
}

func NewInput(
	key string,
	topic string,
	headers []Header,
	payload json.RawMessage,
) Input {
	return Input{
		key:     key,
		topic:   topic,
		headers: headers,
		payload: payload,
	}
}

func (e Input) Headers() []Header {
	return e.headers
}

func (e Input) Topic() string {
	return e.topic
}

func (e Input) Key() string {
	return e.key
}

func (e Input) Payload() json.RawMessage {
	return e.payload
}

type Event = kafka.Event
type FailedEvent = kafka.FailedEvent
type SuccessEvent = kafka.SuccessEvent
type Header = kafka.Header
