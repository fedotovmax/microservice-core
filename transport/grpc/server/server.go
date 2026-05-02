package server

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/fedotovmax/microservice-core/logger"
	"google.golang.org/grpc"
)

type Server interface {
	Start() error
	StartAsync(onError ...func(error))
	Stop(ctx context.Context) error
}

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
func New(c Config, log logger.Logger, registers []RegisterFunc, opts ...grpc.ServerOption) (Server, error) {

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

	l, err := net.Listen("tcp", s.config.Addr)

	if err != nil {
		return fmt.Errorf("%s: tcp listener error: %w", op, err)
	}

	s.mu.Lock()

	switch s.state {
	case stateRunning:
		s.mu.Unlock()
		_ = l.Close() // не забываем закрыть listener
		return fmt.Errorf("%s: server is already running", op)
	case stateStopping:
		s.mu.Unlock()
		_ = l.Close() // не забываем закрыть listener
		return fmt.Errorf("%s: server is currently stopping, wait before restarting", op)
	}

	srv := grpc.NewServer(s.opts...)

	for _, reg := range s.registers {
		reg(srv)
	}

	s.gRPC = srv
	s.state = stateRunning
	s.mu.Unlock()

	s.log.Info("starting gRPC server", logger.String("addr", s.config.Addr))

	err = srv.Serve(l)

	s.mu.Lock()
	if s.gRPC == srv {
		s.gRPC = nil
		s.state = stateStopped
	}
	s.mu.Unlock()

	if err != nil {
		return fmt.Errorf("%s: gRPC runtime error: %w", op, err)
	}

	return nil

}

func (s *gRPCServer) Stop(ctx context.Context) error {
	const op = "gRPCServer.Stop"

	log := s.log.With(logger.String("op", op))

	s.mu.Lock()

	switch s.state {
	case stateStopped:
		s.mu.Unlock()
		return fmt.Errorf("%s: server is not running", op)
	case stateStopping:
		s.mu.Unlock()
		// Остановка уже идёт — повторный вызов игнорируем.
		log.Info("stop already in progress, skipping duplicate call")
		return nil
	}

	s.state = stateStopping
	srv := s.gRPC
	s.mu.Unlock()

	log.Info("stopping gRPC server gracefully...")

	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()

	var stopErr error
	select {
	case <-done:
		log.Info("gRPC server stopped gracefully")
	case <-ctx.Done():
		srv.Stop()
		stopErr = fmt.Errorf("%s: context expired, server force stopped: %w", op, ctx.Err())
		log.Warn("gRPC server force stopped", logger.Err(ctx.Err()))
	}

	// Состояние сбросит Start после возврата из Serve,
	// но подстрахуемся на случай если Stop вызван до завершения горутины Start.
	s.mu.Lock()
	if s.gRPC == srv {
		s.gRPC = nil
		s.state = stateStopped
	}
	s.mu.Unlock()

	return stopErr
}
