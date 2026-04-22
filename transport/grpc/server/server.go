package server

import (
	"context"
	"errors"
	"fmt"
	"net"

	"google.golang.org/grpc"
)

var ErrForceStoppedServer = errors.New("the server was forcibly stopped due to a timeout")

// RegisterFunc — это «клей» между вашим gRPC сервером и сгенерированным кодом
type RegisterFunc func(*grpc.Server)

type gRPCServer struct {
	addr string
	grpc *grpc.Server
}

// Теперь New принимает слайс функций регистрации
func New(addr string, registers []RegisterFunc, opts ...grpc.ServerOption) *gRPCServer {
	s := &gRPCServer{
		addr: addr,
		grpc: grpc.NewServer(opts...),
	}

	// Выполняем регистрацию всех переданных сервисов
	for _, reg := range registers {
		reg(s.grpc)
	}

	return s
}

func (s *gRPCServer) Start() error {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("grpc.Start: %w", err)
	}

	return s.grpc.Serve(listener)
}

func (s *gRPCServer) Stop(ctx context.Context) error {

	const op = "core.transport.grpc.server.gRPCServer.Stop"

	done := make(chan struct{})

	go func() {
		s.grpc.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.grpc.Stop()
		return fmt.Errorf("%s: %w", op, ErrForceStoppedServer)
	}
}
