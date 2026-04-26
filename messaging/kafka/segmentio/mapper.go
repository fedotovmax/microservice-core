package segmentio

import (
	"github.com/fedotovmax/microservice-core/messaging/kafka"
	skafka "github.com/segmentio/kafka-go"
)

func HeadersFromSegmentio(sh []skafka.Header) []kafka.Header {
	res := make([]kafka.Header, len(sh))
	for i, h := range sh {
		res[i] = kafka.Header{Key: []byte(h.Key), Value: h.Value}
	}
	return res
}

func HeadersToSegmentio(kh []kafka.Header) []skafka.Header {
	res := make([]skafka.Header, len(kh))
	for i, h := range kh {
		res[i] = skafka.Header{Key: string(h.Key), Value: h.Value}
	}
	return res
}
