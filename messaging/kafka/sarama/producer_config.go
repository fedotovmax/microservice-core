package sarama

import (
	"fmt"
	"time"

	"github.com/fedotovmax/microservice-core/network"
	"github.com/kelseyhightower/envconfig"
)

type ProducerConfig struct {
	Brokers []string `envconfig:"KAFKA_PRODUCER_BROKERS" required:"true"`
	// Увеличиваем буфер канала. Если он будет меньше BatchLimit из Outbox,
	// метод Send() будет блокироваться и тормозить цикл вычитки из БД.
	ChannelBufferSize int `envconfig:"KAFKA_PRODUCER_CHANNEL_BUFFER_SIZE" default:"256"`

	// Ретраи на уровне Sarama.
	// Для Outbox лучше ставить 3-5, чтобы быстрее получить ошибку и
	// вернуть событие в обработку через БД, а не висеть в памяти продюсера.
	SendMaxRetries int           `envconfig:"KAFKA_PRODUCER_SEND_MAX_RETRIES" default:"3"`
	RetryBackoff   time.Duration `envconfig:"KAFKA_PRODUCER_RETRY_BACKOFF" default:"100ms"`

	// Батчинг (Flush). Эти настройки определяют, когда Sarama реально отправит пакет в сеть.
	BatchFrequency time.Duration `envconfig:"KAFKA_PRODUCER_BATCH_FREQUENCY" default:"100ms"`
	BatchBytes     int           `envconfig:"KAFKA_PRODUCER_BATCH_BYTES" default:"1048576"` // 1MB - хороший стандарт
	// Должен коррелировать с BatchLimit в Outbox.
	// Если в Outbox лимит 100, а тут 300, Sarama будет ждать 3 итерации Outbox или таймаута Frequency.
	BatchMessagesCount int `envconfig:"KAFKA_PRODUCER_BATCH_MESSAGES_COUNT" default:"100"`
}

func (c ProducerConfig) Validate() error {
	const op = "core.messaging.kafka.sarama.ProducerConfig.Validate"

	if len(c.Brokers) == 0 {
		return fmt.Errorf("%s: at least one broker is required", op)
	}

	for i := range c.Brokers {
		if err := network.Addr(c.Brokers[i]); err != nil {
			return fmt.Errorf("%s: invalid broker address %s: %w", op, c.Brokers[i], err)
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
func NewProducerConfig() (ProducerConfig, error) {
	var config ProducerConfig

	if err := envconfig.Process("", &config); err != nil {
		return ProducerConfig{}, fmt.Errorf("error when parse kafka producer env variables: %w", err)
	}

	return config, nil
}

func NewProducerConfigMust() ProducerConfig {
	config, err := NewProducerConfig()

	if err != nil {
		panic(err)
	}

	return config
}
