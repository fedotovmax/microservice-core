package consumer

import "github.com/IBM/sarama"

type msgSaramaCarrier []*sarama.RecordHeader

func (c msgSaramaCarrier) Get(key string) string {
	for _, h := range c {
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}
func (c msgSaramaCarrier) Set(k, v string) {}
func (c msgSaramaCarrier) Keys() []string  { return nil }
