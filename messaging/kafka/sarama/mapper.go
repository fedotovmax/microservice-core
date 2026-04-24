package sarama

import (
	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/messaging/kafka"
)

func CoreHeadersToSarama(headers []kafka.Header) []sarama.RecordHeader {

	saramaHeaders := make([]sarama.RecordHeader, 0, len(headers))

	for _, h := range headers {
		sh := sarama.RecordHeader{Key: h.Key, Value: h.Value}
		saramaHeaders = append(saramaHeaders, sh)
	}

	return saramaHeaders
}

func SaramaHeadersToCore(headers []sarama.RecordHeader) []kafka.Header {
	coreHeaders := make([]kafka.Header, 0, len(headers))

	for _, h := range headers {
		ch := kafka.Header{Key: h.Key, Value: h.Value}
		coreHeaders = append(coreHeaders, ch)
	}

	return coreHeaders
}

func SaramaPtrHeadersToCore(headers []*sarama.RecordHeader) []kafka.Header {
	coreHeaders := make([]kafka.Header, 0, len(headers))

	for _, h := range headers {
		if h == nil {
			continue
		}
		ch := kafka.Header{
			Key:   h.Key,
			Value: h.Value,
		}
		coreHeaders = append(coreHeaders, ch)
	}

	return coreHeaders
}

func extractValue(msg *sarama.ProducerMessage) ([]byte, error) {
	if msg.Value == nil {
		return nil, nil
	}
	return msg.Value.Encode()
}
