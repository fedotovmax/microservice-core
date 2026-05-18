package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/fedotovmax/microservice-core/ft"
)

// waitTopicsReady пингует Kafka, пока ВСЕ переданные топики не будут готовы.
func waitTopicsReady(ctx context.Context, brokers []string, topics []string) error {

	const op = "core.messaging.kafka.sarama.consumer.waitTopicsReady"

	if len(topics) == 0 {
		return nil
	}

	// 1. Настраиваем "вежливый" бэкофф (начинаем с 500мс, максимум 3 секунды)
	backoff := ft.NewExponentialBackoff(500*time.Millisecond, 3*time.Second, 0.2)

	// 2. Сама операция проверки, которую мы будем повторять
	checkTopicsOp := func() error {
		// Настраиваем конфиг Sarama специально для "быстрого" пинга
		config := sarama.NewConfig()
		config.Net.DialTimeout = 2 * time.Second
		config.Net.ReadTimeout = 2 * time.Second
		config.Net.WriteTimeout = 2 * time.Second
		// Отключаем внутренние ретраи Sarama, т.к. мы управляем ими сами через пакет ft
		config.Metadata.Retry.Max = 0

		// Пытаемся подключиться к брокеру
		client, err := sarama.NewClient(brokers, config)
		if err != nil {
			return fmt.Errorf("failed to connect to brokers: %w", err)
		}
		defer client.Close()

		readyCount := 0
		for _, topic := range topics {
			// Запрашиваем партиции для топика.
			// Если auto.create.topics.enable=true, это триггернет создание топика.
			partitions, err := client.Partitions(topic)

			// Топик готов, если нет ошибки и есть хотя бы 1 партиция
			if err == nil && len(partitions) > 0 {
				readyCount++
			}
		}

		if readyCount == len(topics) {
			return nil // УСПЕХ! Выходим из ретрая.
		}

		// Генерируем ошибку, чтобы триггернуть Retry еще раз
		return fmt.Errorf("%s: waiting for topics to be created: ready %d out of %d", op, readyCount, len(topics))
	}

	// 3. Запускаем Retry.
	err := ft.Retry(ctx, backoff, 100, ft.RetryAlwaysPolicy, checkTopicsOp)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}
