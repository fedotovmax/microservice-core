package pgx

import (
	"fmt"
	"time"
)

type BaseConfig struct {
	RetryWaitFrom       time.Duration
	MaxConnLifetime     time.Duration
	MaxIdleConnLifetime time.Duration
	MaxConns            int
	MinConns            int
	MaxRetries          int
}

func (b BaseConfig) Validate() error {
	const op = "core.db.postgresql.pgx.BaseConfig.Validate"

	// 1. Проверка соединений (Connection Pool)
	// MaxConns не может быть меньше 1, иначе приложение не сможет сделать ни одного запроса.
	if b.MaxConns <= 0 {
		return fmt.Errorf("%s: max conns must be at least 1", op)
	}

	// MinConns может быть 0 (ленивое создание соединений), но не меньше.
	if b.MinConns < 0 {
		return fmt.Errorf("%s: min conns cannot be negative", op)
	}

	// Критическая логическая ошибка: пул не заведется, если минимум больше максимума.
	if b.MinConns > b.MaxConns {
		return fmt.Errorf("%s: min conns (%d) cannot be greater than max conns (%d)", op, b.MinConns, b.MaxConns)
	}

	// 2. Проверка жизненного цикла (Lifetimes)
	// Время жизни соединения должно быть положительным, чтобы пул мог ими управлять.
	if b.MaxConnLifetime <= 0 {
		return fmt.Errorf("%s: max conn lifetime must be a positive duration", op)
	}

	// Время простоя (Idle) логично держать меньше или равным общему времени жизни.
	if b.MaxIdleConnLifetime < 0 {
		return fmt.Errorf("%s: max idle conn lifetime cannot be negative", op)
	}

	if b.MaxIdleConnLifetime > b.MaxConnLifetime {
		return fmt.Errorf("%s: max idle conn lifetime cannot exceed MaxConnLifetime", op)
	}

	// 3. Ретраи

	if b.MaxRetries < 0 {
		return fmt.Errorf("%s: max retries cannot be negative", op)
	}

	// Если мы планируем повторять запросы, пауза между ними должна быть физически ощутимой.
	if b.MaxRetries > 0 && b.RetryWaitFrom <= 0 {
		return fmt.Errorf("%s: retry wait from must be positive when retries are enabled", op)
	}

	return nil
}
