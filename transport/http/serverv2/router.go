package serverv2

import (
	"net/http"

	"github.com/fedotovmax/microservice-core/transport/http/middleware"
	"github.com/go-chi/chi/v5"
)

type Router interface {
	RegisterRoute(route Route)
	RegisterRoutes(routes ...Route)
	Use(mw ...middleware.Middleware)
	RouteGroup(pattern string, fn func(Router))
	ServeHTTP(w http.ResponseWriter, req *http.Request)
	Mount(path string, h http.Handler) // Для внешних хендлеров типа Swagger/Prometheus
}

type router struct {
	mux chi.Router
}

func NewRouter() Router {
	mux := chi.NewRouter()
	return &router{mux: mux}
}

func (r *router) RegisterRoute(route Route) {

	finalHandler := middleware.Chain(route.Handler, route.Middlewares...)

	r.mux.Method(route.Method.String(), route.Path, finalHandler)

}

func (r *router) RegisterRoutes(routes ...Route) {

	for _, route := range routes {
		finalHandler := middleware.Chain(route.Handler, route.Middlewares...)

		r.mux.Method(route.Method.String(), route.Path, finalHandler)
	}

}

func (r *router) RouteGroup(path string, fn func(Router)) {
	r.mux.Route(path, func(sub chi.Router) { fn(&router{mux: sub}) })
}

func (r *router) Use(mw ...middleware.Middleware) {
	r.mux.Use(mw...)
}

func (r *router) Mount(path string, h http.Handler) {
	r.mux.Mount(path, h)
}

func (r *router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mux.ServeHTTP(w, req)
}
