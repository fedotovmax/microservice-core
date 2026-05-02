package server

import (
	"fmt"
	"time"
)

type Config struct {
	Addr              string
	ReadTimeout       time.Duration
	ReadHeaderTimeout time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
}

func (c Config) Validate() error {

	const op = "transport.http.server.Config.Validate"

	if c.Addr == "" {
		return fmt.Errorf("%s: grpc listen addr cannot be empty", op)
	}

	return nil
}
