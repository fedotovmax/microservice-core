package serverv2

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/fedotovmax/microservice-core/logger"
)

type HTTPServer struct {
	srv    *http.Server
	log    logger.Logger
	config Config
	mux    http.Handler
}

func New(c Config, log logger.Logger, mux http.Handler) (*HTTPServer, error) {

	if err := c.Validate(); err != nil {
		return nil, err
	}

	return &HTTPServer{
		mux:    mux,
		config: c,
		log:    log,
	}, nil
}

func (s *HTTPServer) Start() error {

	const op = "core.transport.http.server.HTTPServer.Start"

	s.srv = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.config.Port),
		Handler: s.mux,
	}

	s.log.Warn("starting HTTP server", logger.Int("port", s.config.Port))

	err := s.srv.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func (s *HTTPServer) Stop(ctx context.Context) error {

	const op = "core.transport.http.server.HTTPServer.Stop"

	if s.srv == nil {
		return fmt.Errorf("%s: %w", op, ErrCallStopBeforeStartServer)
	}

	s.log.Warn("shutting down HTTP server")

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

	s.log.Warn("HTTP server closed successfully")
	return nil
}
