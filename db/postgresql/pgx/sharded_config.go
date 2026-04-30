package pgx

type ShardedConfig struct {
	BaseConfig
	Shards []string
}
