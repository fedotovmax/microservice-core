package pgx

import (
	"context"
	"fmt"

	"github.com/fedotovmax/microservice-core/db/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pool struct {
	*pgxpool.Pool
	dsn string
}

func New(ctx context.Context, config Config) (postgres.Pool, error) {

	const op = "core.db.postgres.pgx.New"

	pgpool, err := connectWithRetries(ctx, config.BaseConfig, config.Dsn)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	if pgpool == nil {
		return nil, fmt.Errorf("%s: postgres connection is empty", op)
	}

	return &pool{Pool: pgpool, dsn: config.Dsn}, nil

}

func (p *pool) DSN() string {
	return p.dsn
}

func (p *pool) Query(ctx context.Context, sql string, args ...any) (postgres.Rows, error) {
	rows, err := p.Pool.Query(ctx, sql, args...)

	if err != nil {
		return nil, err
	}

	return pgxRows{rows}, nil
}

func (p *pool) QueryRow(ctx context.Context, sql string, args ...any) postgres.Row {
	row := p.Pool.QueryRow(ctx, sql, args...)

	return pgxRow{row}
}

func (p *pool) Exec(ctx context.Context, sql string, args ...any) (postgres.CommandTag, error) {

	cmd, err := p.Pool.Exec(ctx, sql, args...)

	if err != nil {
		return nil, err
	}

	return pgxCmdTag{cmd}, nil
}

func (p *pool) Begin(ctx context.Context) (postgres.Tx, error) {

	tr, err := p.Pool.Begin(ctx)

	if err != nil {
		return nil, err
	}

	return &trx{tr}, nil

}

func (p *pool) BeginTx(ctx context.Context, txOptions postgres.TxOptions) (postgres.Tx, error) {

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

func (p *pool) Stat() postgres.Stat {

	s := p.Pool.Stat()
	return &Stat{Stat: s}

}

func (p *pool) Ping(ctx context.Context) error {

	return p.Pool.Ping(ctx)
}

func (p *pool) Stop(ctx context.Context) error {

	const op = "core.db.postgres.pgx.Pool.Stop"

	if p == nil {
		return fmt.Errorf("%s: %w", op, postgres.ErrWantToCallMethodsAfterInitPool)
	}

	if p.Pool == nil {
		return fmt.Errorf("%s: %w", op, postgres.ErrWantToCallMethodsAfterInitPool)
	}

	done := make(chan struct{})

	go func() {
		defer close(done)
		p.Pool.Close()
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: %w", op, ctx.Err())
	}
}
