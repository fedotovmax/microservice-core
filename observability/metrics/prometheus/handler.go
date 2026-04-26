package prometheus

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const StandartMetricPath = "/metrics"

func Handler() http.Handler {
	return promhttp.Handler()
}
