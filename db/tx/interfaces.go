package tx

import "context"

type Tx interface {
	Wrap(ctx context.Context, fn func(context.Context) error) error
}

// TODO: Add Consistent Hashing maybe later
type ShardedTx interface {
	Wrap(ctx context.Context, key string, fn func(context.Context) error) error
	WithKey(ctx context.Context, key string) context.Context
}
