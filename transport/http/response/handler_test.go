package response

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fedotovmax/microservice-core/logger"
	coreHttp "github.com/fedotovmax/microservice-core/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPResponseHandler_JSON(t *testing.T) {
	t.Run("Success JSON encoding", func(t *testing.T) {
		rw := httptest.NewRecorder()
		log := logger.NewMock()
		h := NewHTTPResponseHandler(log, rw)

		data := map[string]string{"status": "ok"}
		h.JSON(data, http.StatusOK)

		assert.Equal(t, http.StatusOK, rw.Code)
		assert.Equal(t, coreHttp.HeaderContentTypeJSON, rw.Header().Get(coreHttp.HeaderContentType))

		var result map[string]string
		err := json.Unmarshal(rw.Body.Bytes(), &result)
		require.NoError(t, err)
		assert.Equal(t, "ok", result["status"])
	})

	t.Run("Handle encoding error", func(t *testing.T) {
		rw := httptest.NewRecorder()
		log := logger.NewMock()

		h := NewHTTPResponseHandler(log, rw)

		// Тип, который нельзя закодировать в JSON (например, функция)
		invalidData := make(chan int)
		h.JSON(invalidData, http.StatusOK)

		assert.Equal(t, http.StatusInternalServerError, rw.Code)
		assert.Contains(t, rw.Body.String(), "failed to encode json")
	})
}

func TestHTTPResponseHandler_NoContent(t *testing.T) {
	rw := httptest.NewRecorder()
	h := NewHTTPResponseHandler(logger.NewMock(), rw)

	h.NoContent()

	assert.Equal(t, http.StatusNoContent, rw.Code)
	assert.Empty(t, rw.Body.String())
}

func TestHTTPResponseHandler_HandlePanic(t *testing.T) {
	rw := httptest.NewRecorder()
	log := logger.NewMock()
	h := NewHTTPResponseHandler(log, rw)

	panicMsg := "something went wrong"
	userMsg := "Internal server error occurred"

	h.HandlePanic(panicMsg, userMsg)

	assert.Equal(t, http.StatusInternalServerError, rw.Code)
	assert.Equal(t, coreHttp.HeaderContentTypeJSON, rw.Header().Get(coreHttp.HeaderContentType))

	var resp ErrorResponse
	err := json.Unmarshal(rw.Body.Bytes(), &resp)
	require.NoError(t, err)

	assert.Equal(t, userMsg, resp.Message)
	assert.Contains(t, resp.Error, "unexpected panic: something went wrong")
}
