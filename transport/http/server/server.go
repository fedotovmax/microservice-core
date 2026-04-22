package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/fedotovmax/microservice-core/logger"
)

type httpServer struct {
	srv       *http.Server
	log       logger.Logger
	config    Config
	mux       http.Handler
	isRunning atomic.Bool
}

func New(c Config, log logger.Logger, mux http.Handler) (*httpServer, error) {

	if err := c.Validate(); err != nil {
		return nil, err
	}

	return &httpServer{
		mux:    mux,
		config: c,
		log:    log,
	}, nil
}

func (s *httpServer) StartAsync(onError ...func()) {

	const op = "core.transport.grpc.server.httpServer.StartAsync"

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

func (s *httpServer) Start() error {

	const op = "core.transport.http.server.httpServer.Start"

	if !s.isRunning.CompareAndSwap(false, true) {
		return fmt.Errorf("%s: server already running", op)
	}

	defer s.isRunning.Store(false)

	s.srv = &http.Server{
		Addr:    s.config.Addr,
		Handler: s.mux,
	}

	s.log.Warn("starting HTTP server", logger.String("addr", s.config.Addr))

	err := s.srv.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (s *httpServer) Stop(ctx context.Context) error {

	const op = "core.transport.http.server.httpServer.Stop"

	if s.srv == nil {
		return fmt.Errorf("%s: %w", op, ErrCallStopBeforeStartServer)
	}

	log := s.log.With(logger.String("op", op))

	if wasRunning := s.isRunning.Swap(false); !wasRunning {
		log.Warn("cannot stop HTTP server, server not running")
		return nil
	}

	// Пытаемся закрыть красиво
	if err := s.srv.Shutdown(ctx); err != nil {

		// Если не вышло (таймаут или ошибка), закрываем принудительно
		forceErr := s.srv.Close()

		if forceErr != nil {
			return fmt.Errorf("%s: failed to shutdown HTTP server: %w, also failed to force close: %v",
				op, err, forceErr)
		}

		// Если принудительно закрыли успешно, всё равно сообщаем об ошибке Shutdown
		return fmt.Errorf("%s: failed to shutdown HTTP server, but closed forcibly: %w",
			op, err)
	}

	s.log.Warn("HTTP server closed gracefully")
	return nil
}
