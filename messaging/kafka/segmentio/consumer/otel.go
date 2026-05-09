package consumer

import (
	skafka "github.com/segmentio/kafka-go"
)

type msgReaderCarrier []skafka.Header

func (c msgReaderCarrier) Get(key string) string {
	for _, h := range c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
func (c msgReaderCarrier) Set(k, v string) {}
func (c msgReaderCarrier) Keys() []string  { return nil }
