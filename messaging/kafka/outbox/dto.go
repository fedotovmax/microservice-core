package outbox

import (
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

type Input struct {
	key     string
	topic   string
	headers []Header
	payload []byte
}

func NewInput(
	key string,
	topic string,
	headers []Header,
	payload []byte,
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

func (e Input) Payload() []byte {
	return e.payload
}

type Event = kafka.Event
type FailedEvent = kafka.FailedEvent
type SuccessEvent = kafka.SuccessEvent
type Header = kafka.Header
