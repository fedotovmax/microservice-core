package postgresql

import (
	"context"
)

type TxIsoLevel string

const (
	Serializable    TxIsoLevel = "serializable"
	RepeatableRead  TxIsoLevel = "repeatable read"
	ReadCommitted   TxIsoLevel = "read committed"
	ReadUncommitted TxIsoLevel = "read uncommitted"
)

type TxAccessMode string

const (
	ReadWrite TxAccessMode = "read write"
	ReadOnly  TxAccessMode = "read only"
)

type TxDeferrableMode string

const (
	Deferrable    TxDeferrableMode = "deferrable"
	NotDeferrable TxDeferrableMode = "not deferrable"
)

type TxOptions struct {
	IsoLevel       TxIsoLevel
	AccessMode     TxAccessMode
	DeferrableMode TxDeferrableMode
	BeginQuery     string
	CommitQuery    string
}

type Pool interface {
	Executor
	Begin(ctx context.Context) (Tx, error)
	BeginTx(ctx context.Context, txOptions TxOptions) (Tx, error)
	Stop(ctx context.Context) error
}

type Executor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) Row
}

type Tx interface {
	Executor
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

type Rows interface {
	Close()
	Err() error
	Next() bool
	Scan(dest ...any) error
}

type Row interface {
	Scan(dest ...any) error
}

type CommandTag interface {
	RowsAffected() int64
}
