package pgx

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"

	"github.com/fedotovmax/microservice-core/conc"
	"github.com/fedotovmax/microservice-core/db/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

type ShardedPool struct {
	pools []postgres.Pool
	mu    sync.RWMutex
}

func NewSharded(ctx context.Context, config ShardedConfig) (postgres.ShardedPool, error) {
	const op = "core.db.postgres.pgx.NewSharded"

	pgxpools := make([]*pgxpool.Pool, len(config.Shards)) // фиксированный размер

	eg, ctx := conc.NewErrGroupWithContext(ctx)

	for i, dsn := range config.Shards {
		eg.Go(func() error {
			p, err := connectWithRetries(ctx, config.BaseConfig, dsn)
			if err != nil {
				return fmt.Errorf("%s: shard [%d] failed: %w", op, i, err)
			}
			pgxpools[i] = p // безопасно: каждая горутина пишет в свой индекс
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		// закрываем только успешно открытые
		for _, p := range pgxpools {
			if p != nil {
				p.Close()
			}
		}
		return nil, err
	}

	pools := make([]postgres.Pool, len(pgxpools))
	for i := range pgxpools {
		pools[i] = &pool{Pool: pgxpools[i]}
	}

	return &ShardedPool{pools: pools}, nil
}

func (sp *ShardedPool) PingAll(ctx context.Context) error {

	eg, ctx := conc.NewErrGroupWithContext(ctx)

	for i := range sp.pools {
		eg.Go(func() error {
			err := sp.pools[i].Ping(ctx)
			if err != nil {
				return err
			}
			return nil
		})
	}
	return eg.Wait()
}

func (sp *ShardedPool) AddPool(ctx context.Context, config Config) error {

	const op = "core.db.postgres.pgx.ShardedPool.AddPool"

	pgpool, err := connectWithRetries(ctx, config.BaseConfig, config.Dsn)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	if pgpool == nil {
		return fmt.Errorf("%s: postgres connection is empty", op)
	}

	sp.mu.Lock()
	defer sp.mu.Unlock()

	sp.pools = append(sp.pools, &pool{Pool: pgpool})
	return nil
}

func (sp *ShardedPool) RemovePool(ctx context.Context, dsn string) error {

	const op = "core.db.postgres.pgx.ShardedPool.RemovePool"

	sp.mu.Lock()
	defer sp.mu.Unlock()

	for i, p := range sp.pools {
		if p.DSN() == dsn {
			sp.pools = append(sp.pools[:i], sp.pools[i+1:]...)
			if err := p.Stop(ctx); err != nil {
				return fmt.Errorf("%s: pool removed but failed to stop: %w", op, err)
			}
			return nil
		}
	}

	return fmt.Errorf("%s: pool with dsn %q not found", op, dsn)
}

func (sp *ShardedPool) GetPool(key string) postgres.Pool {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	// GetIndex не должен захватывать мьютекс — вычисляем инлайн
	idx := sp.computeIndex(key)
	return sp.pools[idx]
}

func (sp *ShardedPool) GetPoolByIndex(index uint32) postgres.Pool {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return sp.pools[index]
}

func (sp *ShardedPool) computeIndex(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	idx := h.Sum32() % uint32(len(sp.pools))
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
