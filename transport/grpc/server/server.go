package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/fedotovmax/microservice-core/logger"
	"google.golang.org/grpc"
)

// RegisterFunc — это «клей» между вашим gRPC сервером и сгенерированным кодом
type RegisterFunc func(*grpc.Server)

type gRPCServer struct {
	config    Config
	gRPC      *grpc.Server
	isRunning atomic.Bool
	log       logger.Logger
}

// Теперь New принимает слайс функций регистрации
func New(c Config, log logger.Logger, registers []RegisterFunc, opts ...grpc.ServerOption) (*gRPCServer, error) {

	if err := c.Validate(); err != nil {
		return nil, err
	}

	s := &gRPCServer{
		config: c,
		log:    log,
		gRPC:   grpc.NewServer(opts...),
	}

	// Выполняем регистрацию всех переданных сервисов
	for _, reg := range registers {
		reg(s.gRPC)
	}

	return s, nil
}

func (s *gRPCServer) StartAsync(onError ...func()) {

	const op = "core.transport.grpc.server.gRPCServer.StartAsync"

	go func() {
		if err := s.Start(); err != nil {
			s.log.With(logger.String("op", op)).Error("cannot start gRPC server", logger.Err(err))
			if len(onError) > 0 {
				for _, fn := range onError {
					fn()
				}
			}
		}
	}()
}

func (s *gRPCServer) Start() error {

	const op = "core.transport.grpc.server.gRPCServer.Start"

	if !s.isRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("%s: server already running", op)
	}

	defer s.isRunning.Store(false)

	l, err := net.Listen("tcp", s.config.Addr)
	if err != nil {
		return fmt.Errorf("%s: tcp listener error: %w", op, err)
	}

	s.log.With(logger.String("op", op)).Info("starting gRPC server", logger.String("addr", s.config.Addr))

	if err := s.gRPC.Serve(l); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("%s: gRPC runtime error: %w", op, err)
	}

	return nil
}

func (s *gRPCServer) Stop(ctx context.Context) error {

	const op = "core.transport.grpc.server.gRPCServer.Stop"

	log := s.log.With(logger.String("op", op))

	if wasRunning := s.isRunning.Swap(false); !wasRunning {
		log.Warn("cannot stop gRPC server, server not running")
		return nil
	}

	done := make(chan struct{})

	go func() {
		s.gRPC.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		log.Info("gRPC server closed gracefully")
		return nil
	case <-ctx.Done():
		s.gRPC.Stop()
		return fmt.Errorf("%s: gRPC server stop context expired, server closed forcibly", op)
	}
}
