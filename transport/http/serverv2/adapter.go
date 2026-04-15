package serverv2

// import (
// 	"net/http"

// 	"github.com/fedotovmax/microservice-core/transport/http/middleware"
// 	"github.com/go-chi/chi/v5"
// )

// type chiRouter struct {
// 	mux chi.Router
// }

// func newChiRouter(mux chi.Router) *chiRouter {
// 	return &chiRouter{mux: mux}
// }

// func (r *chiRouter) RegisterRoute(route Route) {

// 	finalHandler := middleware.Chain(route.Handler, route.Middlewares...)

// 	r.mux.Method(route.Method.String(), route.Path, finalHandler)

// }

// func (r *chiRouter) RegisterRoutes(routes ...Route) {

// 	for _, route := range routes {
// 		finalHandler := middleware.Chain(route.Handler, route.Middlewares...)

// 		r.mux.Method(route.Method.String(), route.Path, finalHandler)
// 	}

// }

// func (r *chiRouter) Use(mw ...middleware.Middleware) {
// 	r.mux.Use(mw...)
// }

// func (r *chiRouter) RouteGroup(path string, fn func(Router)) {
// 	r.mux.Route(path, func(sub chi.Router) { fn(&chiRouter{mux: sub}) })
// }

// func (r *chiRouter) Mount(path string, h http.Handler) {
// 	r.mux.Mount(path, h)
// }
