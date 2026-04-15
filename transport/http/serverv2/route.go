package serverv2

import (
	"net/http"

	"github.com/fedotovmax/microservice-core/transport/http/middleware"
)

type Method string

const (
	MethodGet    Method = http.MethodGet
	MethodPost   Method = http.MethodPost
	MethodDelete Method = http.MethodDelete
	MethodPut    Method = http.MethodPut
	MethodPatch  Method = http.MethodPatch
)

func (m Method) String() string {
	return string(m)
}

func (m Method) Validate() bool {

	switch m {
	case MethodGet, MethodPost, MethodDelete, MethodPut, MethodPatch:
		return true
	default:
		return false
	}
}

type Route struct {
	Method      Method
	Path        string
	Handler     http.Handler
	Middlewares []middleware.Middleware
}

func ToHandler(fn http.HandlerFunc) http.Handler {
	return http.HandlerFunc(fn)
}

func NewRoute(
	m Method,
	p string,
	h http.HandlerFunc,
) Route {
	return Route{
		Method:  m,
		Path:    p,
		Handler: h,
	}
}
