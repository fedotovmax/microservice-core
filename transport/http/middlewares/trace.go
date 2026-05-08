package middlewares

import (
	"net/http"
	"time"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/transport/http/response"
)

func Trace() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			const op = "core.transport.http.middleware.Trace"

			log := logger.FromContext(r.Context()).With(logger.String("op", op))

			rw := response.NewWriter(w)

			before := time.Now()
			log.Debug("incoming new HTTP request", logger.Time("time", before.UTC()))

			next.ServeHTTP(rw, r)

			log.Debug(
				"HTTP request is done",
				logger.Int("status_code", rw.StatusCode()),
				logger.Duration("latency", time.Since(before)),
			)
		})
	}
}
