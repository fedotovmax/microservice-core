package producer

import (
	skafka "github.com/segmentio/kafka-go"
)

// msgWriterCarrier для проброса traceparent в заголовки Kafka
type msgWriterCarrier struct {
	msg *skafka.Message
}

func (c msgWriterCarrier) Get(k string) string {
	for _, h := range c.msg.Headers {
		if h.Key == k {
			return string(h.Value)
		}
	}
	return ""
}

func (c msgWriterCarrier) Set(k, v string) {
	c.msg.Headers = append(c.msg.Headers, skafka.Header{Key: k, Value: []byte(v)})
}
func (c msgWriterCarrier) Keys() (keys []string) {
	for _, h := range c.msg.Headers {
		keys = append(keys, h.Key)
	}
	return keys
}
