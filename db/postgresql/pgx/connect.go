package pgx

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxDelay = time.Second * 100
const backoffFactor = 2

func connectWithRetries(

	ctx context.Context,
	config BaseConfig,
	dsn string,

) (*pgxpool.Pool, error) {

	const op = "core.db.postgresql.pgx.connectWithRetries"

	parsedConfig, err := pgxpool.ParseConfig(dsn)

	if err != nil {
		return nil, fmt.Errorf("%s: invalid postgres pgx config, cannot parse connection string: %w", op, err)
	}

	parsedConfig.MaxConns = int32(config.MaxConns)
	parsedConfig.MaxConnLifetime = config.MaxConnLifetime
	parsedConfig.MinConns = int32(config.MinConns)
	parsedConfig.MaxConnIdleTime = config.MaxIdleConnLifetime

	db, err := pgxpool.NewWithConfig(ctx, parsedConfig)

	if err != nil {
		return nil, err
	}

	var lastPingError error

	delay := config.RetryWaitFrom

	closeCtx, cancelCloseCtx := context.WithTimeout(context.Background(), time.Second*15)
	defer cancelCloseCtx()

	for i := 1; i <= config.MaxRetries; i++ {
		if ctx.Err() != nil {
			constructorCloseConnection(closeCtx, db)
			return nil, ctx.Err()
		}

		lastPingError = db.Ping(ctx)

		if lastPingError == nil {
			return db, nil
		}

		//TODO: log ping failed

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			constructorCloseConnection(closeCtx, db)
			return nil, fmt.Errorf("%s: %w", op, ctx.Err())
		}
		delay = min(time.Duration(float64(delay)*backoffFactor), maxDelay)
	}
	constructorCloseConnection(closeCtx, db)
	return nil, fmt.Errorf("%s: connection to postgres failed after %d attempts: %w", op, config.MaxRetries, lastPingError)
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
