package tx

import "context"

type Tx interface {
	Wrap(ctx context.Context, fn func(context.Context) error) error
}
