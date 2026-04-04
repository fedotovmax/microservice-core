package tx

import (
	"context"
	"errors"
	"fmt"

	"github.com/fedotovmax/microservice-core/db/postgresql"
	"github.com/fedotovmax/microservice-core/db/tx"
	"github.com/fedotovmax/microservice-core/logger"
)

type Manager struct {
	log  logger.Logger
	pool postgresql.Pool
}

type transaction struct {
	postgresql.Tx
}

func New(conn postgresql.Pool, log logger.Logger) (*Manager, error) {
	if conn == nil {
		return nil, tx.ErrConnRequiredForTx
	}
	return &Manager{
		pool: conn,
		log:  log,
	}, nil
}

func (m *Manager) Wrap(ctx context.Context, fn func(context.Context) error) error {

	m.mustCheckInit()
	return m.wrap(ctx, fn)

}

func (m *Manager) ExtractTx(ctx context.Context) postgresql.Executor {

	m.mustCheckInit()

	executor, ok := ctx.Value(txCtxKey{}).(*transaction)
	if !ok {
		return m.pool
	}

	return executor
}

func (m *Manager) mustCheckInit() {

	const op = "core.db.postgresql.tx.Manger.mustCheckInit"

	if m == nil {
		panic(fmt.Errorf("%s: %w", op, tx.ErrManagerIsNotInit))
	}

	if m.pool == nil {
		panic(fmt.Errorf("%s: %w", op, tx.ErrManagerIsNotInit))
	}

}

func (m *Manager) wrap(ctx context.Context, fn func(context.Context) error) error {

	const op = "core.db.postgresql.tx.Manager.wrap"

	l := m.log.With(logger.String("op", op))

	trx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%s: pool.Begin: cannot start transaction: %w", op, err)
	}

	defer func() {
		rollbackErr := trx.Rollback(ctx)
		if rollbackErr != nil && !errors.Is(rollbackErr, tx.ErrTxClosed) {
			l.Error("rollback failed", logger.Err(rollbackErr))
		} else if rollbackErr == nil {
			l.Debug("transaction rollbacked")
		}
	}()

	ctx = context.WithValue(ctx, txCtxKey{}, &transaction{trx})

	err = fn(ctx)

	if err != nil {
		return fmt.Errorf("%s: error when execute transaction fn: %w", op, err)
	}

	err = trx.Commit(ctx)

	if err != nil {
		return fmt.Errorf("%s: error when commit: %w", op, err)
	}

	return nil
}
