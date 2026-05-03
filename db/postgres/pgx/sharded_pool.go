package pgx

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/fedotovmax/microservice-core/db/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ShardedPool struct {
	pools []postgres.Pool
}

func NewSharded(ctx context.Context, config *ShardedConfig) (postgres.ShardedPool, error) {

	//TODO: maybe add errgroup?
	const op = "core.db.postgres.pgx.NewSharded"

	pgxpools := make([]*pgxpool.Pool, 0, len(config.Shards))

	for i, dsn := range config.Shards {
		p, err := connectWithRetries(ctx, config.BaseConfig, dsn)
		if err != nil {
			for _, opened := range pgxpools {
				opened.Close()
			}
			return nil, fmt.Errorf("%s: shard [%d] failed: %w", op, i, err)
		}

		pgxpools = append(pgxpools, p)
	}

	pools := make([]postgres.Pool, len(pgxpools))

	for i := range pgxpools {
		pools[i] = &pool{Pool: pgxpools[i]}
	}

	return &ShardedPool{
		pools: pools,
	}, nil
}

func (sp *ShardedPool) PingAll(ctx context.Context) error {

	//TODO: add errgroup
	for i := range sp.pools {
		err := sp.pools[i].Ping(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sp *ShardedPool) GetPool(key string) postgres.Pool {
	return sp.pools[sp.GetIndex(key)]
}

func (sp *ShardedPool) GetPoolByIndex(index uint32) postgres.Pool {
	return sp.pools[index]
}

func (m *ShardedPool) GetIndex(key string) uint32 {
	h := fnv.New32a()

	h.Write([]byte(key))

	idx := h.Sum32() % uint32(len(m.pools))

	return idx
}

func (sp *ShardedPool) Stop(ctx context.Context) error {

	const op = "core.db.postgres.pgx.ShardedPool.Stop"

	// TODO: maybe add errgroup?
	for i := range sp.pools {
		if err := sp.pools[i].Stop(ctx); err != nil {
			return fmt.Errorf("%s: failed to stop shard [%d]: %w", op, i, err)
		}
	}
	return nil
}
