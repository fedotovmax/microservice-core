package middleware

import (
	stdhttp "net/http"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/transport/http"
)

func Logger(log logger.Logger) Middleware {
	return func(next stdhttp.Handler) stdhttp.Handler {
		return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
			requestID := r.Header.Get(http.HeaderRequestID)

			l := log.With(
				logger.String("request_id", requestID),
				logger.String("url", r.URL.String()),
			)

			ctx := logger.ToContext(r.Context(), l)

			next.ServeHTTP(w, r.WithContext(ctx))

		})
	}
}
