package server

import (
	"fmt"
	"time"
)

type Config struct {
	Addr string
	//TODO: use pattern optional config
	OnStartErrorHandlersTimeout time.Duration
}

func (c Config) Validate() error {

	const op = "transport.grpc.server.Config.Validate"

	if c.Addr == "" {
		return fmt.Errorf("%s: grpc listen addr cannot be empty", op)
	}

	return nil
}
