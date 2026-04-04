package pgx

import (
	"context"
	"fmt"

	"github.com/fedotovmax/microservice-core/db/postgresql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Pool struct {
	*pgxpool.Pool
}

func New(ctx context.Context, config Config) (postgresql.Pool, error) {

	const op = "core.db.postgresql.pgx.New"

	pool, err := connectWithRetries(ctx, config.Base, config.Dsn)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	if pool == nil {
		return nil, fmt.Errorf("%s: postgres connection is empty", op)
	}

	return &Pool{Pool: pool}, nil

}

func (p *Pool) Query(ctx context.Context, sql string, args ...any) (postgresql.Rows, error) {
	rows, err := p.Pool.Query(ctx, sql, args...)

	if err != nil {
		return nil, err
	}

	return pgxRows{rows}, nil
}

func (p *Pool) QueryRow(ctx context.Context, sql string, args ...any) postgresql.Row {
	row := p.Pool.QueryRow(ctx, sql, args...)

	return pgxRow{row}
}

func (p *Pool) Exec(ctx context.Context, sql string, args ...any) (postgresql.CommandTag, error) {

	cmd, err := p.Pool.Exec(ctx, sql, args...)

	if err != nil {
		return nil, err
	}

	return pgxCmdTag{cmd}, nil
}

func (p *Pool) Begin(ctx context.Context) (postgresql.Tx, error) {

	tr, err := p.Pool.Begin(ctx)

	if err != nil {
		return nil, err
	}

	return &trx{tr}, nil

}

func (p *Pool) BeginTx(ctx context.Context, txOptions postgresql.TxOptions) (postgresql.Tx, error) {

	var pgxTxOptions pgx.TxOptions

	pgxTxOptions.AccessMode = pgx.TxAccessMode(txOptions.AccessMode)
	pgxTxOptions.BeginQuery = txOptions.BeginQuery
	pgxTxOptions.CommitQuery = txOptions.CommitQuery
	pgxTxOptions.DeferrableMode = pgx.TxDeferrableMode(txOptions.DeferrableMode)
	pgxTxOptions.IsoLevel = pgx.TxIsoLevel(txOptions.IsoLevel)

	tr, err := p.Pool.BeginTx(ctx, pgxTxOptions)

	if err != nil {
		return nil, err
	}

	return &trx{tr}, nil
}

func (p *Pool) Stat() postgresql.Stat {

	s := p.Pool.Stat()
	return &Stat{Stat: s}

}

func (p *Pool) Ping(ctx context.Context) error {

	return p.Pool.Ping(ctx)
}

func (p *Pool) Stop(ctx context.Context) error {

	const op = "core.db.postgresql.pgx.Pool.Stop"

	if p == nil {
		return fmt.Errorf("%s: %w", op, postgresql.ErrWantToCallMethodsAfterInitPool)
	}

	if p.Pool == nil {
		return fmt.Errorf("%s: %w", op, postgresql.ErrWantToCallMethodsAfterInitPool)
	}

	done := make(chan struct{})

	go func() {
		defer close(done)
		//p.close()
		p.Pool.Close()
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: %w", op, ctx.Err())
	}

}
