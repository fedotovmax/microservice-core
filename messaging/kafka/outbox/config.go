package outbox

import (
	"fmt"
	"time"
)

type Config struct {
	// BatchLimit — количество событий, вычитываемых из БД за одну итерацию.
	// ВАЖНО: Это значение должно быть МЕНЬШЕ или РАВНО значению
	// KAFKA_PRODUCER_CHANNEL_BUFFER_SIZE.
	// Если батч из БД будет больше буфера продюсера, отправка станет блокирующей,
	// что замедлит обработку и может привести к истечению ReserveDuration.
	BatchLimit int

	// Interval — пауза между итерациями опроса БД.
	Interval time.Duration

	// ReserveDuration — время "заморозки" строки в БД для одного инстанса.
	// Должно быть > (BatchLimit * (SendTimeout + HandleSuccessTimeout)).
	ReserveDuration time.Duration

	// SendTimeout — таймаут на передачу сообщения в канал Sarama.
	SendTimeout time.Duration

	// HandleSuccessTimeout — таймаут на отметку успешной отправки в БД.
	HandleSuccessTimeout time.Duration

	// HandleErrorTimeout — таймаут на отметку ошибки отправки в БД.
	HandleErrorTimeout time.Duration
}

const (
	minLimit    = 10
	maxLimit    = 1000
	minInterval = 350 * time.Millisecond
	// Поднимаем минимальный порог операции до 500мс.
	// 300мс — это слишком "тонко" для сетевых запросов к БД под нагрузкой.
	minOperationTimeout = 500 * time.Millisecond
)

func NewConfig(batchLimit int, interval, reserve, send, success, err time.Duration) (Config, error) {

	const op = "core.messaging.kafka.outbox.NewConfig"

	// 1. Валидация лимитов пачки
	if batchLimit < minLimit || batchLimit > maxLimit {
		return Config{}, fmt.Errorf("%s: batch limit must be between %d and %d", op, minLimit, maxLimit)
	}

	// 2. Валидация интервала опроса БД
	if interval < minInterval {
		return Config{}, fmt.Errorf(
			"%s:interval duration must be at least %d ms",
			op,
			minInterval.Milliseconds(),
		)
	}

	// 3. Исправленная валидация тайм-аутов операций
	// Раньше у тебя было 'if send > minOperationTimeout', что блокировало нормальные значения.
	if send < minOperationTimeout {
		return Config{}, fmt.Errorf("%s:send timeout must be at least %v", op, minOperationTimeout)
	}
	if success < minOperationTimeout {
		return Config{}, fmt.Errorf("%s:success timeout must be at least %v", op, minOperationTimeout)
	}
	if err < minOperationTimeout {
		return Config{}, fmt.Errorf("%s:error timeout must be at least %v", op, minOperationTimeout)
	}

	// 4. Расчет критического времени обработки (Worst Case Scenario)
	// Мы предполагаем, что каждое сообщение в пачке может "висеть" до лимита тайм-аута.
	// Формула: Кол-во сообщений * (Тайм-аут отправки + Тайм-аут подтверждения в БД)
	maxProcessingTime := time.Duration(batchLimit) * (send + success)

	// 5. Валидация ReserveDuration
	// Правило: ReserveDuration должен быть > maxProcessingTime.
	// Если ReserveDuration меньше, то первая запись в пачке "протухнет" в БД
	// и будет подхвачена другим инстансом еще до того, как мы закончим отправку всей пачки.
	if reserve < maxProcessingTime {
		return Config{}, fmt.Errorf(
			"%s:reserve duration (%v) is too short for batch size %d with given timeouts. Minimum safe reserve: %v", op,
			reserve, batchLimit, maxProcessingTime,
		)
	}

	return Config{
		BatchLimit:           batchLimit,
		SendTimeout:          send,
		HandleSuccessTimeout: success,
		HandleErrorTimeout:   err,
		Interval:             interval,
		ReserveDuration:      reserve,
	}, nil
}

func DefaultConfig() Config {
	// Расчет для дефолта:
	// 100 сообщений * (1с send + 1с success) = 200 секунд теоретический максимум.
	// Чтобы не ставить ReserveDuration на 3 минуты, обычно BatchLimit в фоновых процессах
	// либо уменьшают, либо ставят более агрессивные тайм-ауты.

	return Config{
		BatchLimit:           100,
		SendTimeout:          time.Second * 1,
		HandleSuccessTimeout: time.Second * 1,
		HandleErrorTimeout:   time.Second * 1,
		Interval:             time.Second * 2,
		// Ставим 5 минут, чтобы гарантированно успеть обработать пачку даже при тормозах Kafka
		ReserveDuration: time.Minute * 5,
	}
}
