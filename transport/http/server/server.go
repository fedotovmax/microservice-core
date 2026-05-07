package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime/debug"
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
	StartAsync(ctx context.Context, onError func(context.Context, error))
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

// StartAsync — строгая сигнатура: один коллбэк с контекстом, как в gRPC-сервере.
func (s *httpServer) StartAsync(ctx context.Context, onError func(context.Context, error)) {
	const op = "httpServer.StartAsync"
	log := s.log.With(logger.String("op", op))

	go func() {
		err := s.Start()
		if err == nil {
			return
		}

		log.Error("cannot start HTTP server", logger.Err(err))

		if onError != nil {
			func() {
				hCtx, cancel := context.WithTimeout(ctx, s.config.OnStartErrorHandlerTimeout)
				defer cancel()

				defer func() {
					if p := recover(); p != nil {
						log.Error("panic in onError callback",
							logger.Any("panic", p),
							logger.String("stack", string(debug.Stack())),
						)
					}
				}()

				onError(hCtx, err)
			}()
		}
	}()
}

func (s *httpServer) Start() error {
	const op = "httpServer.Start"

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

	err := s.serve(srv)

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
	const op = "httpServer.Stop"

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

	s.state = stateStopping
	srv := s.srv
	s.mu.Unlock()

	log.Info("stopping HTTP server...")

	if err := srv.Shutdown(ctx); err != nil {
		forceErr := srv.Close()

		s.mu.Lock()
		if s.srv == srv {
			s.srv = nil
			s.state = stateStopped
		}
		s.mu.Unlock()

		if forceErr != nil {
			return fmt.Errorf("%s: failed to shutdown: %w, also failed to force close: %v", op, err, forceErr)
		}
		return fmt.Errorf("%s: failed to shutdown, but closed forcibly: %w", op, err)
	}

	s.mu.Lock()
	if s.srv == srv {
		s.srv = nil
		s.state = stateStopped
	}
	s.mu.Unlock()

	log.Info("HTTP server stopped gracefully")
	return nil
}

// serve изолирует панику от ListenAndServe — последний рубеж защиты.
// Паники из конкретных хендлеров лучше перехватывать middleware-уровнем.
func (s *httpServer) serve(srv *http.Server) (err error) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("panic in HTTP server",
				logger.Any("recover", r),
				logger.String("stack", string(debug.Stack())),
			)
			err = fmt.Errorf("panic recovered: %v", r)
		}
	}()

	return srv.ListenAndServe()
}
