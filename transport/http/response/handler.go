package response

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	core_http "github.com/fedotovmax/microservice-core/transport/http"

	"github.com/fedotovmax/microservice-core/logger"
)

type ErrorResponse struct {
	Message string `json:"message" validate:"required"`
	Error   string `json:"error" validate:"required"`
}

type httpResponseHandler struct {
	log logger.Logger
	rw  http.ResponseWriter
}

func NewHTTPResponseHandler(log logger.Logger, rw http.ResponseWriter) *httpResponseHandler {
	return &httpResponseHandler{
		log: log,
		rw:  rw,
	}
}

func (h *httpResponseHandler) HandlePanic(p any, msg string) {

	const op = "core.transport.http.response.HTTPResponseHandler.HandlePanic"

	l := h.log.With(logger.String("op", op))

	err := fmt.Errorf("%s: unexpected panic: %v", op, p)

	l.Error(msg, logger.Err(err))

	response := ErrorResponse{
		Message: msg,
		Error:   err.Error(),
	}

	h.JSON(response, http.StatusInternalServerError)

}

func (h *httpResponseHandler) JSON(body any, statusCode int) {

	h.rw.Header().Set(core_http.HeaderContentType, core_http.HeaderContentTypeJSON)

	var buf bytes.Buffer

	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		http.Error(h.rw, `{"message": "failed to encode json"}`, http.StatusInternalServerError)
		return
	}

	h.rw.WriteHeader(statusCode)
	h.rw.Write(buf.Bytes())
}

func (h *httpResponseHandler) NoContent() {
	h.rw.WriteHeader(http.StatusNoContent)
}
