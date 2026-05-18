package redis

import (
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Cmd — результат Get() с методами конвертации типов.
// Ошибка получения ключа пробрасывается через каждый метод,
// поэтому можно писать: val, err := redis.Get(ctx, rc, "key").Int64()
type Cmd interface {
	String() (string, error)
	Int64() (int64, error)
	Bool() (bool, error)
	Float64() (float64, error)
	Bytes() ([]byte, error)
	JSON(dest any) error
}

type cmd struct {
	sc *redis.StringCmd
}

func newCmd(sc *redis.StringCmd) Cmd {
	return &cmd{sc: sc}
}

func (c *cmd) String() (string, error) {
	return c.sc.Result()
}

func (c *cmd) Int64() (int64, error) {
	return c.sc.Int64()
}

func (c *cmd) Bool() (bool, error) {
	return c.sc.Bool()
}

func (c *cmd) Float64() (float64, error) {
	return c.sc.Float64()
}

func (c *cmd) Bytes() ([]byte, error) {
	return c.sc.Bytes()
}

func (c *cmd) JSON(dest any) error {
	b, err := c.sc.Bytes()
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, dest); err != nil {
		return fmt.Errorf("redis cmd: JSON: %w", err)
	}
	return nil
}
