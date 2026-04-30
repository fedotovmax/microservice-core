package pgx

import (
	"fmt"
	"net/url"
)

type Config struct {
	BaseConfig
	Dsn string
}

func (c Config) Validate() error {
	const op = "core.db.postgresql.pgx.Config.Validate"

	if _, err := url.Parse(c.Dsn); err != nil {
		return fmt.Errorf("%s: invalid postgres connection url: %w", op, err)
	}

	if err := c.BaseConfig.Validate(); err != nil {
		return fmt.Errorf("%s: error when validate base config: %w", op, err)
	}

	return nil

}
