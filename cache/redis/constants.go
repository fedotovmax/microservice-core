package redis

const (
	InfinityTTL = 0
)

const (
	workerPoolSize = 10
)

const (
	opNew = "core.cache.redis.New"

	opPing = "core.cache.redis.client.Ping"

	opSet   = "core.cache.redis.client.Set"
	opSetNX = "core.cache.redis.client.SetIfNotExist"

	opString  = "core.cache.redis.client.String"
	opInt64   = "core.cache.redis.client.Int64"
	opBool    = "core.cache.redis.client.Bool"
	opFloat64 = "core.cache.redis.client.Float64"
	opBytes   = "core.cache.redis.client.Bytes"
	opJSON    = "core.cache.redis.client.JSON"

	opDelete = "core.cache.redis.client.Delete"

	opIncInt64     = "core.cache.redis.client.IncInt64"
	opIncInt64By   = "core.cache.redis.client.IncInt64By"
	opIncFloat64   = "core.cache.redis.client.IncFloat64"
	opIncFloat64By = "core.cache.redis.client.IncFloat64By"

	opHSet    = "core.cache.redis.client.HSet"
	opHGet    = "core.cache.redis.client.HGet"
	opHGetAll = "core.cache.redis.client.HGetAll"

	opPublish    = "core.cache.redis.Publish"
	opSubscribe  = "core.cache.redis.Subscribe"
	op_subscribe = "core.cache.redis.subscribe"

	opStop = "core.cache.redis.client.Stop"
)
