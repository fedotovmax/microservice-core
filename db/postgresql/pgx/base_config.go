package pgx

import "time"

type Base struct {
	Timeout             time.Duration `envconfig:"POSTGRES_TIMEOUT" default:"15s"`
	RetryWaitFrom       time.Duration `envconfig:"POSTGRES_RETRY_WAIT_FROM" default:"5s"`
	MaxConnLifetime     time.Duration `envconfig:"POSTGRES_MAX_CONN_LIFETIME" default:"30m"`
	MaxIdleConnLifetime time.Duration `envconfig:"POSTGRES_MAX_IDLE_CONN_LIFETIME" default:"5m"`
	MaxConns            int           `envconfig:"POSTGRES_MAX_CONNECTIONS" default:"20"`
	MinConns            int           `envconfig:"POSTGRES_MIN_CONNECTIONS" default:"5"`
	MaxRetries          int           `envconfig:"POSTGRES_MAX_RETRIES" default:"5"`
}
