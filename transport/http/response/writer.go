package response

import "net/http"

const (
	UnknownStatusCode = -1
)

type writer struct {
	http.ResponseWriter
	code int
}

func NewWriter(w http.ResponseWriter) *writer {
	return &writer{
		ResponseWriter: w,
		code:           UnknownStatusCode,
	}
}

func (w *writer) WriteHeader(statusCode int) {

	w.ResponseWriter.WriteHeader(statusCode)
	w.code = statusCode

}

func (w *writer) StatusCode() int {

	if w.code == UnknownStatusCode {
		return http.StatusOK
	}

	return w.code
}
