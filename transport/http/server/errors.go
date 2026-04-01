package server

import "errors"

var ErrServerClosedForcibly = errors.New("server closed forcibly")

var ErrUnsupportedRouteMethod = errors.New("unsupported route method")
