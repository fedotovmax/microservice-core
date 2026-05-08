package main

import (
	"context"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/logger/zap"
	"github.com/fedotovmax/microservice-core/transport/http/server"
)

func main() {

	log, err := zap.New(zap.NewConfigMust())

	if err != nil {
		panic(err)
	}

	router := server.NewRouter()

	router.RegisterRoute(server.Route{
		Method: http.MethodGet,
		Path:   "/",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("/ handler working"))
		}),
	})

	httpServer, err := server.New(server.NewConfigMust(":5000"), log, router)

	if err != nil {
		panic(err)
	}

	signalCtx, cancelSignal := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancelSignal()

	serverStartErrChan := make(chan error, 1)

	httpServer.StartAsync(signalCtx, func(ctx context.Context, err error) {
		// тут может быть любая логика
		// в самом конце - пишем в канал ошибку

		serverStartErrChan <- err
	})

	select {
	case startErr := <-serverStartErrChan:
		log.Error("shutting down application due to fatal server error", logger.Err(startErr))
	case <-signalCtx.Done():
		log.Info("shutting down application by system signal")
	}

	log.Info("starting graceful shutdown...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Stop(shutdownCtx); err != nil {
		log.Error("error when stop http server", logger.Err(err))
	}

	log.Info("application stopped successfully")

}
