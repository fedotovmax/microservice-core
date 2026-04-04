package postgresql

import (
	"context"
	"time"
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
	Ping(ctx context.Context) error
	Stat() Stat
	Stop(ctx context.Context) error
}

type ShardedPool interface {
	GetPool(key string) Pool
	GetPoolByIndex(index uint32) Pool
	GetIndex(key string) uint32
	PingAll(ctx context.Context) error
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

type Stat interface {
	AcquireDuration() time.Duration

	AcquiredConns() int32

	CanceledAcquireCount() int64

	ConstructingConns() int32

	EmptyAcquireCount() int64

	IdleConns() int32

	MaxConns() int32

	TotalConns() int32

	NewConnsCount() int64

	MaxLifetimeDestroyCount() int64

	MaxIdleDestroyCount() int64

	EmptyAcquireWaitTime() time.Duration
}
