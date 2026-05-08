package middlewares

import (
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func OtelTrace(serviceName string) Middleware {
	return otelhttp.NewMiddleware(serviceName)
}
