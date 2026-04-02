package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/go-chi/chi/v5"
)

type HTTPServer struct {
	*chiRouter
	log    logger.Logger
	config Config
}

func New(c Config, log logger.Logger) *HTTPServer {

	mux := chi.NewRouter()

	return &HTTPServer{
		chiRouter: newChiRouter(mux),
		config:    c,
		log:       log,
	}
}

func (s *HTTPServer) Start(ctx context.Context) error {

	const op = "core.transport.http.server.HTTPServer.Start"

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", s.config.Port),
		Handler: s.chiRouter.mux,
	}

	signal := make(chan error, 1)

	go func() {

		defer close(signal)

		s.log.Warn("starting HTTP server", logger.String("host", s.config.Host), logger.Int("port", s.config.Port))

		err := srv.ListenAndServe()

		if !errors.Is(err, http.ErrServerClosed) {
			signal <- err
		}
	}()

	select {

	case err := <-signal:

		if err != nil {
			return fmt.Errorf("%s: failed to start HTTP server: %w", op, err)
		}

	case <-ctx.Done():

		s.log.Warn("shutting down HTTP server")

		shutdownCtx, cancelShutdownCtx := context.WithTimeout(context.Background(), s.config.ShutdownTimeout)
		defer cancelShutdownCtx()

		if err := srv.Shutdown(shutdownCtx); err != nil {

			forceErr := srv.Close()

			if forceErr != nil {
				return fmt.Errorf("%s: failed to shutdown HTTP server: %w, also failed to force close: %v", op, err, forceErr)
			}

			return fmt.Errorf("%s: failed to shutdown HTTP server, but closed forcibly: %w: %v", op, ErrServerClosedForcibly, err)

		}

		s.log.Warn("HTTP server closed successfully")

	}

	return nil

}
