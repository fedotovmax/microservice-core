package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/fedotovmax/microservice-core/ft"
	"github.com/segmentio/kafka-go"
)

// WaitTopicsReady пингует Kafka, пока ВСЕ переданные топики не будут готовы.
func waitTopicsReady(ctx context.Context, brokers []string, topics []string) error {

	const op = "core.messaging.kafka.segmentio.consumer.waitTopicsReady"

	if len(topics) == 0 {
		return nil
	}

	client := &kafka.Client{
		Addr:    kafka.TCP(brokers...),
		Timeout: 5 * time.Second,
	}

	// 1. Настраиваем "вежливый" бэкофф (начинаем с 500мс, максимум 3 секунды между пингами)
	backoff := ft.NewExponentialBackoff(500*time.Millisecond, 3*time.Second, 0.2)

	// 2. Сама операция проверки, которую мы будем повторять
	checkTopicsOp := func() error {
		resp, err := client.Metadata(ctx, &kafka.MetadataRequest{
			Topics: topics,
		})

		if err != nil {
			return err // Ошибка сети, брокер не доступен (уйдет в ретрай)
		}

		readyCount := 0
		for _, t := range resp.Topics {
			// Топик готов, если нет системной ошибки и есть хотя бы 1 партиция
			if t.Error == nil && len(t.Partitions) > 0 {
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
	// Передаем большое количество попыток (100), потому что мы полагаемся
	// на отмену по таймауту контекста (ctx.Done()), который ты передаешь снаружи.
	err := ft.Retry(ctx, backoff, 100, ft.RetryAlwaysPolicy, checkTopicsOp)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}
