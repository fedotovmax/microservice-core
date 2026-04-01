package middleware

import (
	"net/http"

	"github.com/fedotovmax/microservice-core/logger"
	"github.com/fedotovmax/microservice-core/transport/http/response"
)

func Panic() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			log := logger.FromContext(r.Context())

			rh := response.NewHTTPResponseHandler(log, w)

			defer func() {
				if err := recover(); err != nil {
					rh.HandlePanic(err, "an unexpected panic occurred while executing an http request")
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}
