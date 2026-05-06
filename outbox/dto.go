package outbox

type Header struct {
	Key   []byte
	Value []byte
}

type Event struct {
	key     string
	topic   string
	payload []byte
	headers []Header
	meta    any
}

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

func NewEvent(
	key string,
	topic string,
	payload []byte,
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

func (e Event) Payload() []byte {
	return e.payload
}
