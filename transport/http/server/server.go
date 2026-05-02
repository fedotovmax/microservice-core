package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"

	"github.com/fedotovmax/microservice-core/logger"
)

type state int

const (
	stateStopped state = iota
	stateRunning
	stateStopping
)

type Server interface {
	Start() error
	StartAsync(onError ...func(error))
	Stop(ctx context.Context) error
}

type httpServer struct {
	mu     sync.Mutex
	state  state
	srv    *http.Server
	log    logger.Logger
	config Config
	mux    http.Handler
}

func New(c Config, log logger.Logger, mux http.Handler) (Server, error) {

	if err := c.Validate(); err != nil {
		return nil, err
	}

	return &httpServer{
		mux:    mux,
		config: c,
		log:    log,
	}, nil
}

func (s *httpServer) StartAsync(onError ...func(error)) {

	const op = "core.transport.http.server.httpServer.StartAsync"

	go func() {
		if err := s.Start(); err != nil {
			s.log.With(logger.String("op", op)).Error("cannot start HTTP server", logger.Err(err))
			if len(onError) > 0 {
				for _, fn := range onError {
					fn(err)
				}
			}
		}
	}()
}

func (s *httpServer) Start() error {

	const op = "core.transport.http.server.httpServer.Start"

	s.mu.Lock()

	switch s.state {
	case stateRunning:
		s.mu.Unlock()
		return fmt.Errorf("%s: server is already running", op)
	case stateStopping:
		s.mu.Unlock()
		return fmt.Errorf("%s: server is currently stopping, wait before restarting", op)
	}

	srv := &http.Server{
		Addr:              s.config.Addr,
		Handler:           s.mux,
		ReadTimeout:       s.config.ReadTimeout,
		ReadHeaderTimeout: s.config.ReadHeaderTimeout,
		WriteTimeout:      s.config.WriteTimeout,
		IdleTimeout:       s.config.IdleTimeout,
	}
	s.srv = srv
	s.state = stateRunning
	s.mu.Unlock()

	s.log.Info("starting HTTP server", logger.String("addr", s.config.Addr))

	err := srv.ListenAndServe()

	// ListenAndServe завершился — сбрасываем состояние.
	// Проверяем s.srv == srv, чтобы не затереть состояние от нового запуска.
	s.mu.Lock()
	if s.srv == srv {
		s.srv = nil
		s.state = stateStopped
	}
	s.mu.Unlock()

	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (s *httpServer) Stop(ctx context.Context) error {

	const op = "core.transport.http.server.httpServer.Stop"

	log := s.log.With(logger.String("op", op))

	s.mu.Lock()

	switch s.state {
	case stateStopped:
		s.mu.Unlock()
		return fmt.Errorf("%s: server is not running", op)
	case stateStopping:
		s.mu.Unlock()
		log.Info("stop already in progress, skipping duplicate call")
		return nil
	}

	// Переходим в промежуточное состояние
	s.state = stateStopping
	srv := s.srv
	s.mu.Unlock() // Отпускаем лок до Shutdown, чтобы не блокировать Start.

	log.Info("stopping HTTP server...")

	if err := srv.Shutdown(ctx); err != nil {
		// Graceful shutdown не удался — закрываем принудительно.
		forceErr := srv.Close()

		// Состояние сбросит Start, когда ListenAndServe вернёт управление.
		// Но если Start по какой-то причине не отреагировал — подстрахуемся.
		s.mu.Lock()
		if s.srv == srv {
			s.srv = nil
			s.state = stateStopped
		}
		s.mu.Unlock()

		if forceErr != nil {
			return fmt.Errorf("%s: failed to shutdown HTTP server: %w, also failed to force close: %v",
				op, err, forceErr)
		}

		// Если принудительно закрыли успешно, всё равно сообщаем об ошибке Shutdown
		return fmt.Errorf("%s: failed to shutdown HTTP server, but closed forcibly: %w",
			op, err)
	}

	// Успешное завершение
	s.mu.Lock()
	if s.srv == srv {
		s.srv = nil
		s.state = stateStopped
	}
	s.mu.Unlock()

	log.Info("HTTP server closed gracefully")
	return nil
}
