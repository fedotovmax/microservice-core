package pgx

import (
	"context"
	"fmt"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/fedotovmax/microservice-core/ft"
	"github.com/jackc/pgx/v5/pgxpool"
)

func connectWithRetries(
	ctx context.Context,
	config BaseConfig,
	dsn string,
) (*pgxpool.Pool, error) {
	const op = "core.db.postgres.pgx.connectWithRetries"

	parsedConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("%s: invalid postgres config: %w", op, err)
	}

	parsedConfig.MaxConns = int32(config.MaxConns)
	parsedConfig.MaxConnLifetime = config.MaxConnLifetime
	parsedConfig.MinConns = int32(config.MinConns)
	parsedConfig.MaxConnIdleTime = config.MaxIdleConnLifetime

	if config.Tracing {
		parsedConfig.ConnConfig.Tracer = otelpgx.NewTracer()
	}

	// Создаем пул. Важно: pgxpool.NewWithConfig не устанавливает соединение сразу,
	// он просто инициализирует структуру. Настоящая проверка идет через Ping.
	db, err := pgxpool.NewWithConfig(ctx, parsedConfig)
	if err != nil {
		return nil, fmt.Errorf("%s: pool creation failed: %w", op, err)
	}

	// Настраиваем наш новый Backoff
	// Используем параметры из твоего конфига
	bo := ft.NewExponentialBackoff(
		config.RetryWaitFrom,
		time.Second*100, // maxDelay
		0.1,             // небольшой jitter для здоровья базы
	)

	// Определяем операцию, которую будем ретраить
	operation := func() error {
		return db.Ping(ctx)
	}

	// Запускаем наш универсальный Retry
	// Используем ft.RetryAlwaysPolicy, если хотим ретраить любую ошибку пинга
	err = ft.Retry(ctx, bo, config.MaxRetries, ft.RetryAlwaysPolicy, operation)

	if err != nil {
		// Если все попытки провалены или контекст отменен
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second*15)
		defer cancel()

		constructorCloseConnection(closeCtx, db)
		return nil, fmt.Errorf("%s: connection failed after %d attempts: %w", op, config.MaxRetries, err)
	}

	return db, nil
}

func constructorCloseConnection(ctx context.Context, db *pgxpool.Pool) {

	if db == nil {
		return
	}

	done := make(chan struct{})

	go func() {
		defer close(done)
		db.Close()
	}()

	select {
	case <-done:
		return
	case <-ctx.Done():
		return
	}

}
