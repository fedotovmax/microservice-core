package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/fedotovmax/microservice-core/logger"
	"google.golang.org/grpc"
)

type state int

const (
	stateStopped state = iota
	stateRunning
	stateStopping
)

// RegisterFunc — это «клей» между вашим gRPC сервером и сгенерированным кодом
type RegisterFunc func(*grpc.Server)

type gRPCServer struct {
	config    Config
	gRPC      *grpc.Server
	mu        sync.Mutex
	state     state
	log       logger.Logger
	registers []RegisterFunc
	opts      []grpc.ServerOption
}

// Теперь New принимает слайс функций регистрации
func New(c Config, log logger.Logger, registers []RegisterFunc, opts ...grpc.ServerOption) (*gRPCServer, error) {

	if err := c.Validate(); err != nil {
		return nil, err
	}

	return &gRPCServer{
		config:    c,
		log:       log,
		registers: registers,
		opts:      opts,
	}, nil

}

func (s *gRPCServer) StartAsync(onError ...func(error)) {

	const op = "core.transport.grpc.server.gRPCServer.StartAsync"

	go func() {
		if err := s.Start(); err != nil {
			s.log.With(logger.String("op", op)).Error("cannot start gRPC server", logger.Err(err))
			if len(onError) > 0 {
				for _, fn := range onError {
					fn(err)
				}
			}
		}
	}()
}

func (s *gRPCServer) Start() error {

	const op = "core.transport.grpc.server.gRPCServer.Start"

	s.mu.Lock()

	if s.state != stateStopped {
		msg := "already running"
		if s.state == stateStopping {
			msg = "currently stopping"
		}
		s.mu.Unlock()
		return fmt.Errorf("%s: server is %s", op, msg)
	}

	// Инициализируем сервер именно в момент старта
	// Это позволяет "перезапускать" сервер, если он был остановлен

	srv := grpc.NewServer(s.opts...)
	for _, reg := range s.registers {
		reg(srv)
	}

	s.gRPC = srv
	s.state = stateRunning
	s.mu.Unlock()

	// Создаем listener вне мьютекса
	l, err := net.Listen("tcp", s.config.Addr)
	if err != nil {
		s.mu.Lock()
		s.state = stateStopped
		s.gRPC = nil
		s.mu.Unlock()
		return fmt.Errorf("%s: tcp listener error: %w", op, err)
	}

	s.log.Warn("starting gRPC server", logger.String("addr", s.config.Addr))

	// Serve блокирует горутину
	err = srv.Serve(l)

	s.mu.Lock()
	if s.gRPC == srv {
		s.gRPC = nil
		s.state = stateStopped
	}
	s.mu.Unlock()

	if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return fmt.Errorf("%s: gRPC runtime error: %w", op, err)
	}

	return nil

}

func (s *gRPCServer) Stop(ctx context.Context) error {
	const op = "core.transport.grpc.server.gRPCServer.Stop"

	s.mu.Lock()
	if s.state != stateRunning {
		s.mu.Unlock()
		return fmt.Errorf("%s: cannot stop, server not running", op)
	}

	s.state = stateStopping
	srv := s.gRPC
	s.mu.Unlock()

	s.log.Warn("stopping gRPC server gracefully...")

	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()

	var stopErr error
	select {
	case <-done:
		s.log.Warn("gRPC server closed gracefully")
	case <-ctx.Done():
		// Если контекст истек, рубим соединения жестко
		srv.Stop()
		stopErr = fmt.Errorf("%s: stop context expired, server closed forcibly: %w", op, ctx.Err())
	}

	s.mu.Lock()
	s.state = stateStopped
	s.gRPC = nil
	s.mu.Unlock()

	return stopErr
}
