package response

import "net/http"

const (
	UnknownStatusCode = -1
)

type Writer struct {
	http.ResponseWriter
	code int
}

func NewWriter(w http.ResponseWriter) *Writer {
	return &Writer{
		ResponseWriter: w,
		code:           UnknownStatusCode,
	}
}

func (w *Writer) WriteHeader(statusCode int) {

	w.ResponseWriter.WriteHeader(statusCode)
	w.code = statusCode

}

func (w *Writer) StatusCode() int {

	if w.code == UnknownStatusCode {
		return http.StatusOK
	}

	return w.code
}
