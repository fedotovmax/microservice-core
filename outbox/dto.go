package outbox

type Header struct {
	Key   []byte
	Value []byte
}

type Event struct {
	routingKey   string
	destination  string
	payload      []byte
	headers      []Header
	internalMeta any
}

type FailedEvent struct {
	internalMeta any
	err          error
	headers      []Header
}

func NewFailedEvent(meta any, h []Header, err error) FailedEvent {
	return FailedEvent{internalMeta: meta, err: err, headers: h}
}

func (e FailedEvent) Headers() []Header {
	return e.headers
}

func (e FailedEvent) InternalMeta() any {
	return e.internalMeta
}

func (e FailedEvent) Error() error {
	return e.err
}

type SuccessEvent struct {
	internalMeta any
	headers      []Header
}

func NewSuccessEvent(meta any, h []Header) SuccessEvent {
	return SuccessEvent{headers: h, internalMeta: meta}
}

func (e SuccessEvent) Headers() []Header {
	return e.headers
}

func (e SuccessEvent) InternalMeta() any {
	return e.internalMeta
}

func NewEvent(
	routingKey string,
	destination string,
	payload []byte,
	headers []Header,
	internalMeta any,
) Event {
	return Event{
		routingKey:   routingKey,
		destination:  destination,
		payload:      payload,
		headers:      headers,
		internalMeta: internalMeta,
	}
}

func (e Event) Headers() []Header {
	return e.headers
}

func (e Event) InternalMeta() any {
	return e.internalMeta
}

func (e Event) Destination() string {
	return e.destination
}

func (e Event) RoutingKey() string {
	return e.routingKey
}

func (e Event) Payload() []byte {
	return e.payload
}
