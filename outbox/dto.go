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
	ev  Event
	err error
}

func NewFailedEvent(e Event, err error) FailedEvent {
	return FailedEvent{ev: e, err: err}
}

func (e FailedEvent) Event() Event {
	return e.ev
}

func (e FailedEvent) Error() error {
	return e.err
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
