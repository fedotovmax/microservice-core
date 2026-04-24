package kafka

import (
	"encoding/json"
	"fmt"
)

type FailedEvent struct {
	meta    any
	err     error
	headers []Header
}

func NewFailedEvent(meta any, h []Header, err error) FailedEvent {
	return FailedEvent{meta: meta, err: err, headers: h}
}

func (e FailedEvent) GetHeaders() []Header {
	return e.headers
}

func (e FailedEvent) GetMeta() any {
	return e.meta
}

func (e FailedEvent) GetError() error {
	return e.err
}

type SuccessEvent struct {
	meta    any
	headers []Header
}

func NewSuccessEvent(meta any, h []Header) SuccessEvent {
	return SuccessEvent{headers: h, meta: meta}
}

func (e SuccessEvent) GetHeaders() []Header {
	return e.headers
}

func (e SuccessEvent) GetMeta() any {
	return e.meta
}

type Header struct {
	Key   []byte
	Value []byte
}

type Event struct {
	key     string
	topic   string
	payload json.RawMessage
	headers []Header
	meta    any
}

func NewEvent(
	key string,
	topic string,
	payload json.RawMessage,
	headers []Header,
	meta any,
) Event {
	return Event{
		key:     key,
		topic:   topic,
		payload: payload,
		headers: headers,
		meta:    meta,
	}
}

func (e Event) Headers() []Header {
	return e.headers
}

func (e Event) Meta() any {
	return e.meta
}

func (e Event) Topic() string {
	return e.topic
}

func (e Event) Key() string {
	return e.key
}

func (e Event) Payload() json.RawMessage {
	return e.payload
}

type ConsumeEvent struct {
	payload   json.RawMessage
	key       []byte
	offset    int64
	topic     string
	partition int32
	headers   []Header
}

func NewConsumeEvent(
	payload json.RawMessage,
	key []byte,
	offset int64,
	topic string,
	partition int32,
	headers []Header,
) ConsumeEvent {
	return ConsumeEvent{
		payload:   payload,
		key:       key,
		offset:    offset,
		topic:     topic,
		partition: partition,
		headers:   headers,
	}
}

func (e *ConsumeEvent) Payload() json.RawMessage {
	return e.payload
}

func (e *ConsumeEvent) Key() []byte {
	return e.key
}

func (e *ConsumeEvent) Offset() int64 {
	return e.offset
}

func (e *ConsumeEvent) Topic() string {
	return e.topic
}

func (e *ConsumeEvent) Partition() int32 {
	return e.partition
}

func (e *ConsumeEvent) Headers() []Header {
	return e.headers
}

// Вернуть эту ошибку, если например в хедерах не оказалось нужных значений
// Нужно, чтобы mark сделать с текстом.
type NoRetryError struct {
	Reason string
}

func NewNoRetryError(r string) *NoRetryError {
	return &NoRetryError{Reason: r}
}

func (e *NoRetryError) Error() string {
	return fmt.Sprintf("message does not need to be processed again, because: %s", e.Reason)
}

type ConsumerGroupStartReadParams struct {
	MessageHandler
	OnCleanUp
	OnSetup
	Middlewares []Middleware
}
