package pgx

import (
	"context"
	"errors"

	"github.com/fedotovmax/microservice-core/db/postgres"
	"github.com/fedotovmax/microservice-core/db/tx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pgxRows struct {
	pgx.Rows
}

type pgxRow struct {
	pgx.Row
}

func (r pgxRow) Scan(dest ...any) error {
	err := r.Row.Scan(dest...)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return postgres.ErrNoRows
		}
		return err
	}

	return nil
}

type pgxCmdTag struct {
	pgconn.CommandTag
}

type trx struct {
	pgx.Tx
}

func (t *trx) Rollback(ctx context.Context) error {
	err := t.Tx.Rollback(ctx)

	if err != nil {
		if errors.Is(err, pgx.ErrTxClosed) {
			return tx.ErrTxClosed
		}
		return err
	}
	return nil
}

func (t *trx) Query(ctx context.Context, sql string, args ...any) (postgres.Rows, error) {
	rows, err := t.Tx.Query(ctx, sql, args...)

	if err != nil {
		return nil, err
	}

	return pgxRows{rows}, nil
}

func (t *trx) QueryRow(ctx context.Context, sql string, args ...any) postgres.Row {
	row := t.Tx.QueryRow(ctx, sql, args...)

	return pgxRow{row}
}

func (t *trx) Exec(ctx context.Context, sql string, args ...any) (postgres.CommandTag, error) {

	cmd, err := t.Tx.Exec(ctx, sql, args...)

	if err != nil {
		return nil, err
	}

	return pgxCmdTag{cmd}, nil
}

type Stat struct {
	*pgxpool.Stat
}
