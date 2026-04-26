package prometheus

import (
	"net/http"
	"strconv"
	"time"

	"github.com/fedotovmax/microservice-core/transport/http/middleware"
	"github.com/fedotovmax/microservice-core/transport/http/request"
	"github.com/fedotovmax/microservice-core/transport/http/response"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	httpRequestCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total HTTP requests",
		},
		[]string{"method", "path", "status"},
	)

	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	httpErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_errors_total",
			Help: "Total number of HTTP error responses",
		},
		[]string{"method", "path", "status"},
	)
)

func RegisterHTTPMetrics() {
	prometheus.MustRegister(
		httpRequestCount,
		httpRequestDuration,
		httpErrorsTotal,
	)
}

func Metrics() middleware.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			start := time.Now()

			rw := response.NewWriter(w)

			next.ServeHTTP(rw, r)

			duration := time.Since(start).Seconds()

			routePattern := request.RoutePattern(r.Context())

			status := rw.StatusCode()

			httpRequestDuration.
				WithLabelValues(r.Method, routePattern).
				Observe(duration)

			httpRequestCount.
				WithLabelValues(
					r.Method,
					routePattern,
					strconv.Itoa(status),
				).
				Inc()

			if status >= 400 {
				httpErrorsTotal.
					WithLabelValues(
						r.Method,
						routePattern,
						strconv.Itoa(status),
					).
					Inc()
			}
		})
	}
}
