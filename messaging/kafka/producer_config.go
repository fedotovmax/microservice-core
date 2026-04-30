package kafka

import (
	"fmt"
	"time"
)

type ProducerConfig struct {
	Brokers []string
	// Увеличиваем буфер канала. Если он будет меньше BatchLimit из Outbox,
	// метод Send() будет блокироваться и тормозить цикл вычитки из БД.
	ChannelBufferSize int

	// Ретраи на уровне Sarama.
	// Для Outbox лучше ставить 3-5, чтобы быстрее получить ошибку и
	// вернуть событие в обработку через БД, а не висеть в памяти продюсера.
	SendMaxRetries int
	RetryBackoff   time.Duration
	// Батчинг (Flush). Эти настройки определяют, когда Sarama реально отправит пакет в сеть.
	BatchFrequency time.Duration
	BatchBytes     int // 1MB - хороший стандарт
	// Должен коррелировать с BatchLimit в Outbox.
	// Если в Outbox лимит 100, а тут 300, Sarama будет ждать 3 итерации Outbox или таймаута Frequency.
	BatchMessagesCount int
}

func (c ProducerConfig) Validate() error {
	const op = "core.messaging.kafka.ProducerConfig.Validate"

	if len(c.Brokers) == 0 {
		return fmt.Errorf("%s: at least one broker is required", op)
	}

	for i := range c.Brokers {
		if c.Brokers[i] == "" {
			return fmt.Errorf("%s: broker address with index: %d is empty", op, i)
		}
	}

	// Минимально необходимый ретрай — 1.
	// Если 0, Sarama при первой же сетевой заминке выкинет ошибку в HandleErrors.
	if c.SendMaxRetries < 1 {
		return fmt.Errorf("%s: send max retries must be at least 1", op)
	}

	if c.RetryBackoff < time.Millisecond*10 {
		return fmt.Errorf("%s: retry backoff is too aggressive, min 10ms", op)
	}

	// Если ChannelBufferSize будет 0, канал будет небуферизированным.
	// В AsyncProducer это приведет к тому, что каждое сообщение будет ждать
	// подтверждения приема горутиной Sarama, что уничтожит производительность.
	if c.ChannelBufferSize < 100 {
		return fmt.Errorf("%s: channel buffer size must be at least 100 for stable async work", op)
	}

	if c.BatchMessagesCount <= 0 {
		return fmt.Errorf("%s: batch messages count must be greater than 0", op)
	}

	return nil
}
