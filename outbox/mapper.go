package outbox

import (
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

func HeadersToKafka(oh []Header) []kafka.Header {
	kafkaHeaders := make([]kafka.Header, 0, len(oh))

	for _, h := range oh {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{
			Key:   h.Key,
			Value: h.Value,
		})
	}

	return kafkaHeaders
}

func HeadersFromKafka(kh []kafka.Header) []Header {
	outboxHeaders := make([]Header, 0, len(kh))

	for _, h := range kh {
		outboxHeaders = append(outboxHeaders, Header{
			Key:   h.Key,
			Value: h.Value,
		})
	}

	return outboxHeaders
}
