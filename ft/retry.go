package ft

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

type Backoff interface {
	Next(attempt int) time.Duration
}

type ExponentialBackoff struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Jitter    float64
}

func NewExponentialBackoff(baseDelay, maxDelay time.Duration, jitter float64) *ExponentialBackoff {
	return &ExponentialBackoff{
		BaseDelay: baseDelay,
		MaxDelay:  maxDelay,
		Jitter:    jitter,
	}
}

func (b *ExponentialBackoff) Next(attempt int) time.Duration {
	// 1. Экспонента: Base * 2^attempt
	// Используем сдвиг для эффективности или math.Pow
	exp := math.Pow(2, float64(attempt))
	delay := float64(b.BaseDelay) * exp

	// 2. Ограничиваем сверху сразу
	if b.MaxDelay > 0 && delay > float64(b.MaxDelay) {
		delay = float64(b.MaxDelay)
	}

	// 3. Jitter (Full Jitter approach или диапазонный)
	if b.Jitter > 0 {
		// Чтобы не уйти в минус, считаем отклонение от текущего delay
		spread := delay * b.Jitter
		// Диапазон: delay +/- spread
		randomPart := (rand.Float64()*2 - 1) * spread
		delay += randomPart
	}

	// 4. Финальная защита: задержка не может быть меньше базовой (или 0)
	if delay < float64(b.BaseDelay) {
		delay = float64(b.BaseDelay)
	}

	return time.Duration(delay)
}

type RetryPolicy func(error) bool

func RetryAlwaysPolicy(error) bool {
	return true
}

func Retry(ctx context.Context, b Backoff, max int, policy RetryPolicy, op func() error) error {
	var lastErr error

	for i := 0; i < max; i++ {

		if err := ctx.Err(); err != nil {
			return err
		}

		if lastErr = op(); lastErr == nil {
			return nil
		}

		// Если политика говорит не пробовать или это была последняя попытка
		if (policy != nil && !policy(lastErr)) || i == max-1 {
			return lastErr
		}

		wait := b.Next(i)

		// Используем таймер, чтобы избежать утечек памяти
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
			// продолжаем цикл
		case <-ctx.Done():
			timer.Stop() // Обязательно останавливаем таймер
			return ctx.Err()
		}
	}

	return lastErr
}
