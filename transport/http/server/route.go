package server

import (
	"net/http"

	"github.com/fedotovmax/microservice-core/transport/http/middlewares"
)

type Route struct {
	Method      string
	Path        string
	Handler     http.Handler
	Middlewares []middlewares.Middleware
}

func ToHandler(fn http.HandlerFunc) http.Handler {
	return http.HandlerFunc(fn)
}
