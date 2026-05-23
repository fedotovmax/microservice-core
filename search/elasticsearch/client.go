package elasticsearch

import (
	"context"
	"fmt"
	"sync"

	"github.com/elastic/go-elasticsearch/v9"
	"go.opentelemetry.io/otel"
)

type client struct {
	el        *elasticsearch.TypedClient
	addresses []string
}

var (
	once     sync.Once
	initErr  error
	instance Client
)

func New(cfg Config) (Client, error) {

	once.Do(func() {
		var opts []elasticsearch.Option

		switch cfg.AuthMethod {
		case AuthMethodBearerToken:
			if cfg.AuthByBearerToken != nil {
				opts = append(opts, elasticsearch.WithServiceToken(cfg.AuthByBearerToken.Token))
			}
		case AuthMethodCredentials:
			if cfg.AuthByCredentials != nil {
				opts = append(opts, elasticsearch.WithBasicAuth(cfg.AuthByCredentials.Username, cfg.AuthByCredentials.Passwrod))
			}
		}

		opts = append(opts, elasticsearch.WithAddresses(cfg.Addresses...))
		opts = append(opts, elasticsearch.WithRetry(cfg.MaxRetries))

		if cfg.Telemetry {
			opts = append(opts, elasticsearch.WithInstrumentation(
				elasticsearch.NewOpenTelemetryInstrumentation(
					otel.GetTracerProvider(),
					cfg.TelemetryShowSearchBodyInTraces,
				),
			))
		}

		typedClient, err := elasticsearch.NewTyped(opts...)

		if err != nil {
			initErr = err
			return
		}

		instance = &client{el: typedClient, addresses: cfg.Addresses}

	})

	if initErr != nil {
		return nil, initErr
	}

	return instance, nil
}

func (c *client) Native() *elasticsearch.TypedClient {

	return c.el
}

func (c *client) Stop(ctx context.Context) error {

	const op = "elastic.Stop"

	done := make(chan error, 1)

	go func() {
		done <- c.el.Close(ctx)
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("%s: %w", op, err)
		}
		// if c.log != nil {
		// 	c.log.With(logger.String("op", op)).Info("Redis connection closed gracefully")
		// }
		return nil
	case <-ctx.Done():
		return fmt.Errorf("%s: %w", op, ctx.Err())
	}
}
