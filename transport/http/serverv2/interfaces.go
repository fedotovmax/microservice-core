package serverv2

import (
	"net/http"

	"github.com/fedotovmax/microservice-core/transport/http/middleware"
)

type Router interface {
	RegisterRoute(route Route)
	RegisterRoutes(routes ...Route)
	Use(mw ...middleware.Middleware)
	RouteGroup(pattern string, fn func(Router))
	ServeHTTP(w http.ResponseWriter, req *http.Request)
	Mount(path string, h http.Handler) // Для внешних хендлеров типа Swagger/Prometheus
}
