package tx

import (
	"context"
	"errors"
	"fmt"

	"github.com/fedotovmax/microservice-core/db/postgresql"
	"github.com/fedotovmax/microservice-core/db/tx"
	"github.com/fedotovmax/microservice-core/logger"
)

type shardedManager struct {
	log  logger.Logger
	pool postgresql.ShardedPool
}

// type shardedTransaction struct {
// 	postgresql.Tx
// 	shardIdx uint32
// }

func NewSharded(pool postgresql.ShardedPool, log logger.Logger) (postgresql.TxShardedManager, error) {

	if pool == nil {
		return nil, tx.ErrConnRequiredForTx
	}

	return &shardedManager{
		pool: pool,
		log:  log,
	}, nil
}

func (m *shardedManager) WrapWithOptions(ctx context.Context, key string, fn func(context.Context) error, opt postgresql.TxOptions) error {

	const op = "core.db.postgresql.tx.Manger.WrapWithOptions"

	idx := m.pool.GetIndex(key)

	p := m.pool.GetPoolByIndex(idx)

	trx, err := p.BeginTx(ctx, opt)
	if err != nil {
		return fmt.Errorf("%s: pool.Begin: cannot start transaction: %w", op, err)
	}

	m.mustCheckInit()
	return m.wrap(ctx, trx, fn)

}

func (m *shardedManager) Wrap(ctx context.Context, key string, fn func(context.Context) error) error {

	const op = "core.db.postgresql.tx.Manger.Wrap"

	idx := m.pool.GetIndex(key)

	p := m.pool.GetPoolByIndex(idx)

	trx, err := p.Begin(ctx)
	if err != nil {
		return fmt.Errorf("%s: pool.Begin: cannot start transaction: %w", op, err)
	}

	m.mustCheckInit()
	return m.wrap(ctx, trx, fn)

}

func (m *shardedManager) WithKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, shardKeyCtxKey{}, key)
}

func (m *shardedManager) ExtractTx(ctx context.Context) (postgresql.Executor, error) {

	m.mustCheckInit()

	t, ok := ctx.Value(txCtxKey{}).(postgresql.Tx)

	if ok {
		return t, nil
	}

	if key, ok := ctx.Value(shardKeyCtxKey{}).(string); ok {
		return m.pool.GetPool(key), nil
	}

	return nil, tx.ErrNoShardContext

}

func (m *shardedManager) mustCheckInit() {

	const op = "core.db.postgresql.tx.Manger.mustCheckInit"

	if m == nil {
		panic(fmt.Errorf("%s: %w", op, tx.ErrManagerIsNotInit))
	}

	if m.pool == nil {
		panic(fmt.Errorf("%s: %w", op, tx.ErrManagerIsNotInit))
	}

}

func (m *shardedManager) wrap(ctx context.Context, trx postgresql.Tx, fn func(context.Context) error) error {

	const op = "core.db.postgresql.tx.Manager.wrap"

	l := m.log.With(logger.String("op", op))

	defer func() {
		rollbackErr := trx.Rollback(ctx)
		if rollbackErr != nil && !errors.Is(rollbackErr, tx.ErrTxClosed) {
			l.Error("rollback failed", logger.Err(rollbackErr))
		} else if rollbackErr == nil {
			l.Debug("transaction rollbacked")
		}
	}()

	ctx = context.WithValue(ctx, txCtxKey{}, trx)

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
