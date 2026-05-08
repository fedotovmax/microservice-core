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

func NewMessageFromProducer(msg *sarama.ProducerMessage, meta any) (kafka.Message, error) {

	key, err := msg.Key.Encode()
	if err != nil {
		return kafka.Message{}, err
	}

	value, err := msg.Value.Encode()
	if err != nil {
		return kafka.Message{}, err
	}

	return kafka.NewMessage(string(key), msg.Topic, value, HeadersFromSarama(msg.Headers), meta), nil

}

func NewFailedMessageFromProducer(msg *sarama.ProducerError, meta any) (kafka.FailedMessage, error) {

	km, err := NewMessageFromProducer(msg.Msg, meta)

	if err != nil {
		return kafka.FailedMessage{}, nil
	}

	return kafka.NewFailedMessage(km, msg.Err), nil

}
