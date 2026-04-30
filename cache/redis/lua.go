package redis

import "github.com/redis/go-redis/v9"

var (
	luaIncrInt = redis.NewScript(`
		local val = redis.call("INCRBY", KEYS[1], ARGV[1])
		if val == tonumber(ARGV[1]) then
			local expire_ms = tonumber(ARGV[2])
			if expire_ms > 0 then
				redis.call("PEXPIRE", KEYS[1], expire_ms)
			end
		end
		return val
	`)

	luaIncrFloat = redis.NewScript(`
		local val = redis.call("INCRBYFLOAT", KEYS[1], ARGV[1])
		if tonumber(val) == tonumber(ARGV[1]) then
			local expire_ms = tonumber(ARGV[2])
			if expire_ms > 0 then
				redis.call("PEXPIRE", KEYS[1], expire_ms)
			end
		end
		return val
	`)
)
