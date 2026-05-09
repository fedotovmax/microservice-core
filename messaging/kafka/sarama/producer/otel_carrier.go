package producer

import (
	"github.com/IBM/sarama"
)

// Идеальный адаптер без явных разыменований указателей
type saramaMsgCarrier struct {
	msg *sarama.ProducerMessage
}

func (c saramaMsgCarrier) Get(key string) string { return "" }
func (c saramaMsgCarrier) Set(k, v string) {
	// Никаких звездочек! Красиво и читаемо.
	c.msg.Headers = append(c.msg.Headers, sarama.RecordHeader{
		Key:   []byte(k),
		Value: []byte(v),
	})
}
func (c saramaMsgCarrier) Keys() []string { return nil }

// Обертка для метадаты, чтобы пронести спан сквозь асинхронные каналы
