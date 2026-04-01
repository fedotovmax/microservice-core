package middleware

import (
	stdhttp "net/http"

	"github.com/fedotovmax/microservice-core/transport/http"
	"github.com/google/uuid"
)

func RequestID() Middleware {
	return func(next stdhttp.Handler) stdhttp.Handler {
		return stdhttp.HandlerFunc(func(w stdhttp.ResponseWriter, r *stdhttp.Request) {
			requestID := r.Header.Get(http.HeaderRequestID)

			if requestID == "" {
				requestID = uuid.NewString()
			}

			r.Header.Set(http.HeaderRequestID, requestID)
			w.Header().Set(http.HeaderRequestID, requestID)

			next.ServeHTTP(w, r)

		})
	}
}
