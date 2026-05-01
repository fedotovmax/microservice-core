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

type httpServer struct {
	mu     sync.Mutex
	state  state
	srv    *http.Server
	log    logger.Logger
	config Config
	mux    http.Handler
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

	if s.state != stateStopped {
		msg := "already running"
		if s.state == stateStopping {
			msg = "currently stopping"
		}
		s.mu.Unlock()
		return fmt.Errorf("%s: server is %s", op, msg)
	}

	srv := &http.Server{
		Addr:    s.config.Addr,
		Handler: s.mux,
	}
	s.srv = srv
	s.state = stateRunning
	s.mu.Unlock()

	s.log.Warn("starting HTTP server", logger.String("addr", s.config.Addr))

	err := srv.ListenAndServe()

	// После завершения ListenAndServe (ошибка или Shutdown) — очищаем состояние
	s.mu.Lock()
	// Очищаем только если это тот же самый экземпляр сервера, который мы запускали
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

	if s.state != stateRunning {
		s.mu.Unlock()
		return fmt.Errorf("%s: cannot stop, server not running", op)
	}

	// Переходим в промежуточное состояние
	s.state = stateStopping
	srv := s.srv
	s.mu.Unlock() // Отпускаем лок, чтобы не блокировать Start на 30 секунд

	log.Warn("stopping HTTP server...")

	// Пытаемся закрыть красиво
	if err := srv.Shutdown(ctx); err != nil {

		// Если не вышло (таймаут или ошибка), закрываем принудительно
		forceErr := srv.Close()

		// Финальный сброс состояния даже при ошибке
		s.mu.Lock()
		s.state = stateStopped
		s.srv = nil
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
	s.state = stateStopped
	s.srv = nil
	s.mu.Unlock()

	log.Warn("HTTP server closed gracefully")
	return nil
}
