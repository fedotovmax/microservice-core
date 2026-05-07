package kafka

import (
	"fmt"
)

type FailedMessage struct {
	meta    any
	err     error
	headers []Header
}

func NewFailedMessage(meta any, h []Header, err error) FailedMessage {
	return FailedMessage{meta: meta, err: err, headers: h}
}

func (e FailedMessage) Headers() []Header {
	return e.headers
}

func (e FailedMessage) Meta() any {
	return e.meta
}

func (e FailedMessage) Error() error {
	return e.err
}

type SuccessMessage struct {
	meta    any
	headers []Header
}

func NewSuccessMessage(meta any, h []Header) SuccessMessage {
	return SuccessMessage{headers: h, meta: meta}
}

func (e SuccessMessage) Headers() []Header {
	return e.headers
}

func (e SuccessMessage) Meta() any {
	return e.meta
}

type Header struct {
	Key   []byte
	Value []byte
}

type Message struct {
	key     string
	topic   string
	payload []byte
	headers []Header
	meta    any
}

func NewMessage(
	key string,
	topic string,
	payload []byte,
	headers []Header,
	meta any,
) Message {
	return Message{
		key:     key,
		topic:   topic,
		payload: payload,
		headers: headers,
		meta:    meta,
	}
}

func (e Message) Headers() []Header {
	return e.headers
}

func (e Message) Meta() any {
	return e.meta
}

func (e Message) Topic() string {
	return e.topic
}

func (e Message) Key() string {
	return e.key
}

func (e Message) Payload() []byte {
	return e.payload
}

type ConsumeMessage struct {
	payload   []byte
	key       []byte
	offset    int64
	topic     string
	partition int32
	headers   []Header
}

func NewConsumeMessage(
	payload []byte,
	key []byte,
	offset int64,
	topic string,
	partition int32,
	headers []Header,
) ConsumeMessage {
	return ConsumeMessage{
		payload:   payload,
		key:       key,
		offset:    offset,
		topic:     topic,
		partition: partition,
		headers:   headers,
	}
}

func (e *ConsumeMessage) Payload() []byte {
	return e.payload
}

func (e *ConsumeMessage) Key() []byte {
	return e.key
}

func (e *ConsumeMessage) Offset() int64 {
	return e.offset
}

func (e *ConsumeMessage) Topic() string {
	return e.topic
}

func (e *ConsumeMessage) Partition() int32 {
	return e.partition
}

func (e *ConsumeMessage) Headers() []Header {
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
