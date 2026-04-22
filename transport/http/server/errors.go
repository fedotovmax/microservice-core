package server

import "errors"

var ErrServerClosedForcibly = errors.New("server closed forcibly")

var ErrUnsupportedRouteMethod = errors.New("unsupported route method")

var ErrCallStopBeforeStartServer = errors.New("http is not starting: call Start() before call Stop")
