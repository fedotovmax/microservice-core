package tx

import (
	"context"
	"errors"
	"fmt"

	"github.com/fedotovmax/microservice-core/db/postgresql"
	"github.com/fedotovmax/microservice-core/db/tx"
	"github.com/fedotovmax/microservice-core/logger"
)

type txCtxKey struct{}

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

	if m == nil {
		panic("manager is not initialized: call New() before using methods")
	}

	if m.pool == nil {
		panic("manager is not initialized: call New() before using methods")
	}

}

func (m *Manager) wrap(ctx context.Context, fn func(context.Context) error) error {
	trx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("pool.Begin: cannot start transaction: %w", err)
	}

	defer func() {
		rollbackErr := trx.Rollback(ctx)
		if rollbackErr != nil && !errors.Is(rollbackErr, tx.ErrTxClosed) {
			//TODO: log
			m.log.Error("rollback failed", logger.Err(rollbackErr))
		} else if rollbackErr == nil {
			m.log.Debug("transaction rollbacked")
		}
	}()

	ctx = context.WithValue(ctx, txCtxKey{}, &transaction{trx})

	err = fn(ctx)

	if err != nil {
		return fmt.Errorf("error when execute transaction fn: %w", err)
	}

	err = trx.Commit(ctx)

	if err != nil {
		return fmt.Errorf("error when commit: %w", err)
	}

	return nil
}
