package sarama

import (
	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

func HeadersToSarama(h []kafka.Header) []sarama.RecordHeader {

	hs := make([]sarama.RecordHeader, 0, len(h))

	for idx := range h {
		hs = append(hs, sarama.RecordHeader{Key: h[idx].Key, Value: h[idx].Value})
	}
	return hs
}

func HeadersFromSarama(h []sarama.RecordHeader) []kafka.Header {

	hs := make([]kafka.Header, 0, len(h))

	for idx := range h {
		hs = append(hs, kafka.Header{Key: h[idx].Key, Value: h[idx].Value})
	}

	return hs
}

func HeadersFromPtrSarama(h []*sarama.RecordHeader) []kafka.Header {

	hs := make([]kafka.Header, 0, len(h))

	for idx := range h {
		if h[idx] == nil {
			continue
		}
		hs = append(hs, kafka.Header{
			Key:   h[idx].Key,
			Value: h[idx].Value,
		})
	}

	return hs
}

// TODO: maybe add payload for Success, Failed Events??
func ExtractValue(msg *sarama.ProducerMessage) ([]byte, error) {
	if msg.Value == nil {
		return nil, nil
	}
	return msg.Value.Encode()
}
