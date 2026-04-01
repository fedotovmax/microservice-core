package outbox

import (
	"fmt"
	"time"
)

type Config struct {
	BatchLimit       int
	Interval         time.Duration
	ReserveDuration  time.Duration
	OperationTimeout time.Duration
}

const (
	minLimit            = 10
	maxLimit            = 1000
	minInterval         = 100 * time.Millisecond
	minReserveDuration  = 10 * time.Second
	minOperationTimeout = 100 * time.Millisecond
)

func NewConfig(batchLimit int, interval, reserve, operation time.Duration) (Config, error) {

	if batchLimit < minLimit || batchLimit > maxLimit {
		return Config{}, fmt.Errorf("batch limit must be greater than or equal to %d and less than or equal %d", minLimit, maxLimit)
	}

	if interval < minInterval {
		return Config{}, fmt.Errorf(
			"interval duration must be greater than or equal to %d milliseconds",
			minInterval.Milliseconds(),
		)
	}

	if reserve < minReserveDuration {
		return Config{}, fmt.Errorf(
			"reserve duration must be greater than or equal to %.2f seconds",
			minReserveDuration.Seconds(),
		)
	}

	if operation > minOperationTimeout {
		return Config{}, fmt.Errorf(
			"operation duration must be greater than or equal to %d milliseconds",
			minOperationTimeout.Milliseconds(),
		)
	}

	return Config{
		BatchLimit:       batchLimit,
		ReserveDuration:  reserve,
		Interval:         interval,
		OperationTimeout: operation,
	}, nil
}

func DefaultConfig() Config {
	return Config{
		BatchLimit:       50,
		Interval:         time.Second * 1,
		ReserveDuration:  time.Second * 20,
		OperationTimeout: time.Millisecond * 150,
	}
}
