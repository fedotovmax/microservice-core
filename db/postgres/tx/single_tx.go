package tx

import (
	"context"
	"errors"
	"fmt"

	"github.com/fedotovmax/microservice-core/db/postgres"
	"github.com/fedotovmax/microservice-core/db/tx"
	"github.com/fedotovmax/microservice-core/logger"
)

type singleManager struct {
	log  logger.Logger
	pool postgres.Pool
}

type transaction struct {
	postgres.Tx
}

func New(conn postgres.Pool, log logger.Logger) (postgres.TxManager, error) {

	if conn == nil {
		return nil, tx.ErrConnRequiredForTx
	}
	return &singleManager{
		pool: conn,
		log:  log,
	}, nil
}

func (m *singleManager) WrapWithOptions(ctx context.Context, fn func(context.Context) error, opt postgres.TxOptions) error {

	const op = "core.db.postgres.tx.Manager.WrapWithOptions"

	trx, err := m.pool.BeginTx(ctx, opt)
	if err != nil {
		return fmt.Errorf("%s: cannot start transaction with options: %w", op, err)
	}

	return m.wrap(ctx, trx, fn)
}

func (m *singleManager) Wrap(ctx context.Context, fn func(context.Context) error) error {

	const op = "core.db.postgres.tx.Manager.Wrap"

	trx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%s: cannot start transaction: %w", op, err)
	}

	return m.wrap(ctx, trx, fn)

}

func (m *singleManager) ExtractTx(ctx context.Context) postgres.Executor {

	executor, ok := ctx.Value(txCtxKey{}).(*transaction)
	if !ok {
		return m.pool
	}

	return executor
}

func (m *singleManager) wrap(ctx context.Context, trx postgres.Tx, fn func(context.Context) error) error {

	const op = "core.db.postgres.tx.Manager.wrap"

	l := m.log.With(logger.String("op", op))

	defer func() {
		rollbackErr := trx.Rollback(ctx)
		if rollbackErr != nil && !errors.Is(rollbackErr, tx.ErrTxClosed) {
			l.Error("rollback failed", logger.Err(rollbackErr))
		} else if rollbackErr == nil {
			l.Debug("transaction rollbacked")
		}
	}()

	ctx = context.WithValue(ctx, txCtxKey{}, &transaction{trx})

	err := fn(ctx)

	if err != nil {
		return fmt.Errorf("%s: error when execute transaction fn: %w", op, err)
	}

	err = trx.Commit(ctx)

	if err != nil {
		return fmt.Errorf("%s: error when commit: %w", op, err)
	}

	return nil
}
