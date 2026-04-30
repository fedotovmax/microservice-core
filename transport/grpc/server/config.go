package server

import (
	"fmt"
)

type Config struct {
	Addr string
}

func (c Config) Validate() error {

	const op = "transport.grpc.server.Config.Validate"

	if c.Addr == "" {
		return fmt.Errorf("%s: grpc listen addr cannot be empty", op)
	}

	return nil
}
