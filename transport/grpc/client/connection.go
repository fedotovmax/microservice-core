package client

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type clientConnGRPC struct {
	conn *grpc.ClientConn
}

func NewClientConn(addr string, opts ...grpc.DialOption) (*clientConnGRPC, error) {
	// Если опции не переданы, ставим insecure по умолчанию,
	// чтобы клиент не упал при попытке найти TLS-сертификаты
	if len(opts) == 0 {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}

	return &clientConnGRPC{conn: conn}, nil
}

func (c *clientConnGRPC) Stop(ctx context.Context) error {

	const op = "core.transport.grpc.client.clientConnGRPC.Stop"

	done := make(chan error, 1)

	go func() {
		err := c.conn.Close()
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func GetClient[T any](mgr *clientConnGRPC, constructor func(grpc.ClientConnInterface) T) T {
	return constructor(mgr.conn)
}
