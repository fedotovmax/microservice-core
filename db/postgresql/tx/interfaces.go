package tx

import (
	"context"

	"github.com/fedotovmax/microservice-core/db/postgresql"
)

type Extractor interface {
	ExtractTx(ctx context.Context) postgresql.Executor
}
