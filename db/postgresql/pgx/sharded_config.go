package pgx

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

type ShardedConfig struct {
	Base
	Shards []string `envconfig:"POSTGRES_SHARDS" required:"true"`
}

func NewShardedConfig() (ShardedConfig, error) {
	const op = "core.db.postgresql.pgx.NewShardedConfig"

	var config ShardedConfig

	if err := envconfig.Process("", &config); err != nil {
		return ShardedConfig{}, fmt.Errorf("%s: error when parse sharded postgres env variables: %w", op, err)
	}

	return config, nil
}

func NewShardedConfigMust() ShardedConfig {
	config, err := NewShardedConfig()
	if err != nil {
		panic(err)
	}
	return config
}
